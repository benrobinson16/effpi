// Effpi - verified message-passing programs in Dotty
// Copyright 2019 Alceste Scalas and Elias Benussi
// Released under the MIT License: https://opensource.org/licenses/MIT
package effpi.system

import java.lang.Runnable

import scala.util.{Failure, Success, Try}
import scala.concurrent.duration.Duration

import effpi.channel.{InChannel, OutChannel, ChannelStatus}
import effpi.waiting._

protected[system] class InputExecutor(ps: ProcessSystem, stepsLeft: Int = 10) extends Runnable {

  import effpi.process._

  override def run() = {
    while (ps.alive) {
      val maybeInCh = ps.consumeInCh()
      maybeInCh match {
        case Some(in) =>
          ps match {
            case _: ProcessSystemRunnerImproved =>
              ()
            case _: ProcessSystemStateMachineMultiStep =>
              in.schedulingStatus.set(ChannelStatus.running)
          }
          in.dequeue() match {
            case Some(waitingProc) =>
              handleDequeueOp(in, waitingProc)

            case None =>
              ps match {
                case _: ProcessSystemRunnerImproved =>
                  ps.scheduleInCh(in)
                case _: ProcessSystemStateMachineMultiStep =>
                  ps.smartUnschedule(in)
              }
          }
        case None =>
          ()
      }
    }
  }

  private def handleDequeueOp(
    in: InChannel[Any],
    wp: WaitingProcess
  ): Unit = {
    wp.state match {
      case WaitingState.Resolved =>
        // This op has already been handled, so we do not re-enqueue it.
        // We should reschedule the channel to handle other ops
        // that might be waiting (as there might be a value available).
        ps match {
          case _: ProcessSystemRunnerImproved => ps.scheduleInCh(in)
          case _: ProcessSystemStateMachineMultiStep => ps.smartEnqueue(in)
        }

      case WaitingState.Claiming =>
        // Another thread is fulfilling the op.
        // Re-enqueue because that other thread could fail to receive a value.
        in.enqueue(wp)

        // We need to reschedule! We need to ensure this channel's input
        // gets run again when we can actually claim this op, as there might
        // be a value available to be received.
        ps match {
          case _: ProcessSystemRunnerImproved => ps.scheduleInCh(in)
          case _: ProcessSystemStateMachineMultiStep => ps.smartEnqueue(in)
        }

      case WaitingState.Pending =>
        if (wp.tryClaim()) {
          // We have claimed the op, try to receive
          if (wp.deadlineExpired) {
            wp.markResolved()
            ps.cancelTimeout(wp.id)
            wp.channels.foreach(_.removeWaitingProcById(wp.id))
            wp.onTimeout match {
              case Some(c) => multiInEval((wp.env, wp.lp, c()), stepsLeft - 1)
              case None => throw RuntimeException("Deadline expired but no onTimeout continuation provided.")
            }
          } else {
            // Deadline not yet passed
            in.poll() match {
              case Some(v) =>
                // Got a value!
                wp.markResolved()
                ps.cancelTimeout(wp.id)
                wp.channels.filter(_ ne in).foreach(_.removeWaitingProcById(wp.id))
                val cont = wp.continuation(in, v, ps)
                multiInEval((wp.env, wp.lp, cont), stepsLeft - 1)

                // FIXME Explain this
                ps match {
                  case _: ProcessSystemRunnerImproved => ()
                  case _: ProcessSystemStateMachineMultiStep => ps.forceSchedule(in)
                }

              case None =>
                // No value available, revert to Pending and re-enqueue
                wp.markPending()
                in.enqueue(wp)

                // Don't reschedule as we have checked there is no value
                // available. The next sending operation will reschedule us.
                ps match {
                  case _: ProcessSystemRunnerImproved => ()
                  case _: ProcessSystemStateMachineMultiStep => ps.smartUnschedule(in)
                }
            }
          }
        } else {
          // Lost to another thread, re-enqueue.
          in.enqueue(wp)

          // We should reschedule because there might be a value available
          // on the channel (we haven't been able to check).
          ps match {
            case _: ProcessSystemRunnerImproved => ps.scheduleInCh(in)
            case _: ProcessSystemStateMachineMultiStep => ps.smartEnqueue(in)
          }
        }
    }
  }

  @annotation.tailrec
  private def multiInEval(
    proc: (Map[ProcVar[_], (_) => Process], List[() => Process], Process),
    stepsLeft: Int
  ): Unit = {
    if (stepsLeft == 0) {
      ps.scheduleProc(proc)
    } else {
      val (env, lp, p) = proc
      p match {
        case i: In[_,_,_] => {
          // Handle in a helper function...
          handleReadingOp(
            WaitingProcess(env, lp, i, deadline=None, onTimeout=None),
            stepsLeft
          )
        }
        case b: Branch[_,_,_] => {
          // Handle in a helper function...
          handleReadingOp(
            WaitingProcess(env, lp, b, deadline=None, onTimeout=None),
            stepsLeft
          )
        }
        case ct: CatchTimeout[_, _] => {
          // Extract the timeout from the inner process
          val inner = ct.p()
          val timeout = inner match {
            case i: In[_, _, _] => i.timeout
            case b: Branch[_, _, _] => b.timeout
          }

          val deadline = if (timeout.isFinite) Some(System.nanoTime() + timeout.toNanos) else None
          val onTimeout = if (timeout.isFinite) Some(ct.onTimeout) else None

          handleReadingOp(
            WaitingProcess(env, lp, inner, deadline, onTimeout),
            stepsLeft
          )
        }
        case o: Out[_,_] => {
          val outCh = o.channel.asInstanceOf[OutChannel[Any]]
          val ack: Option[InChannel[Unit]] = if (outCh.synchronous) {
            // Send an ack channel together with the value
            val ack = outCh.create[Unit](false) // The ack channel *must* be async
            outCh.send((o.v, ack.out))
            Some(ack.in)
          } else {
            outCh.send(o.v)
            None
          }
          val inCh = outCh.dualIn
          ps match {
            case _: ProcessSystemRunnerImproved =>
              ps.scheduleInCh(inCh)
            case _: ProcessSystemStateMachineMultiStep =>
              ps.smartEnqueue(inCh)
          }
          ack match {
            case Some(inc) => {
              // We must now reschedule ourselves, while we wait for an ack
              // FIXME: allow to specify timeouts
              import effpi.process.dsl.{receive, nil}
              import concurrent.duration.Duration.Inf
              multiInEval((env, lp, receive(inc){ _ => nil }(Inf)), stepsLeft)
            }
            case None => {
              lp match {
                case Nil => ()
                case lh :: lt => multiInEval((env, lt, lh()), stepsLeft - 1)
              }
            }
          }
        }
        case f: Fork[_] => {
          // TODO: this always gives the same order? this may or may not be a problem
          ps.scheduleProc((env, Nil, f.p()))
          lp match {
            case Nil => ()
            case lh :: lt =>
              multiInEval((env, lt, lh()), stepsLeft - 1)
          }
        }
        case n: PNil => lp match {
          case Nil => ()
          case lh :: lt =>
            multiInEval((env, lt, lh()), stepsLeft - 1)
        }
        case y: Yield[_] => {
          y.ctx match {
            case Some(c) => c.chan.asInstanceOf[OutChannel[Any]].send(y.v)
            case None => ()
          }
          lp match {
            case Nil => ()
            case lh :: lt =>
              multiInEval((env, lt, lh()), stepsLeft - 1)
          }
        }
        case d: Def[_,_,_,_] =>
          multiInEval((env + (d.name -> d.pdef), lp, d.in()), stepsLeft - 1)
        case c: Call[_,_] => {
          env.get(c.procvar) match {
            case Some(p) =>
              multiInEval(
                (env, lp, p.asInstanceOf[Any => Process](c.arg)), stepsLeft - 1)
            case None =>
              throw new RuntimeException(s"Unbound process variable: ${c.procvar}")
          }
        }
        case s: >>:[_,_] =>
          multiInEval((env, s.p2 :: lp, s.p1()), stepsLeft - 1)
      }
    }
  }

  private def handleReadingOp(
    wp: WaitingProcess,
    stepsLeft: Int
  ): Unit = {
    wp.poll() match {
      case Some((ch, v)) =>
        // We have a value! Get the continuation for this value
        // and evaluate it.
        val cont = wp.continuation(ch, v, ps)
        ps match {
          case _: ProcessSystemRunnerImproved => ()
          case _: ProcessSystemStateMachineMultiStep => ps.forceSchedule(ch)
        }
        multiInEval((wp.env, wp.lp, cont), stepsLeft - 1)

      case None =>
        // No value available, enqueue
        wp.channels.foreach(_.enqueue(wp))
        wp.scheduleTimerIfNeeded(ps)
        ps match {
          case _: ProcessSystemRunnerImproved => ()
          case _: ProcessSystemStateMachineMultiStep => wp.channels.foreach(ps.smartUnschedule)
        }
    }
  }
}
