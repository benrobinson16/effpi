// Effpi - verified message-passing programs in Dotty
// Copyright 2019 Alceste Scalas and Elias Benussi
// Released under the MIT License: https://opensource.org/licenses/MIT
package effpi.system

import java.lang.Runnable

import scala.util.{Failure, Success, Try}
import scala.concurrent.duration.Duration

import effpi.channel.{InChannel, OutChannel}
import effpi.waiting._

protected[system] class Executor(ps: ProcessSystem, stepsLeft: Int = 10) extends Runnable {

  import effpi.process._

  override def run() = {
    while (ps.alive) {
      val head = ps.consumeProc()
      head match {
        case Some(p) =>
          fastEval(p, stepsLeft)
        case None =>
          ()
      }
    }
  }

  @annotation.tailrec
  private def fastEval(
    proc: (Map[ProcVar[_], (_) => Process], List[() => Process], Process),
    stepsLeft: Int
  ): Unit = {
    if (stepsLeft <= 0) {
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
            fastEval((env, lp, receive(inc){ _ => nil }(Inf)), stepsLeft)
          }
          case None => {
            lp match {
              case Nil => ()
              case lh :: lt => fastEval((env, lt, lh()), stepsLeft - 1)
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
            fastEval((env, lt, lh()), stepsLeft - 1)
        }
      }
      case n: PNil => lp match {
        case Nil => ()
        case lh :: lt =>
          fastEval((env, lt, lh()), stepsLeft - 1)
      }
      case y: Yield[_] => {
        y.ctx match {
          case Some(c) => c.chan.asInstanceOf[OutChannel[Any]].send(y.v)
          case None => ()
        }
        lp match {
          case Nil => ()
          case lh :: lt =>
            fastEval((env, lt, lh()), stepsLeft - 1)
        }
      }
      case d: Def[_,_,_,_] =>
        fastEval((env + (d.name -> d.pdef), lp, d.in()), stepsLeft - 1)
      case c: Call[_,_] => {
        env.get(c.procvar) match {
          case Some(p) =>
            fastEval(
              (env, lp, p.asInstanceOf[Any => Process](c.arg)), stepsLeft - 1)
          case None =>
            throw new RuntimeException(s"Unbound process variable: ${c.procvar}")
        }
      }
      case s: >>:[_,_] =>
        fastEval((env, s.p2 :: lp, s.p1()), stepsLeft - 1)
      }
    }
  }

  private def handleReadingOp(
    wp: WaitingProcess,
    stepsLeft: Int
  ): Unit = {
    ps match {
      case _: ProcessSystemRunnerImproved =>
        // Enqueue and schedule timeout
        wp.channels.foreach(_.enqueue(wp))
        wp.scheduleTimerIfNeeded(ps)
      
      case _: ProcessSystemStateMachineMultiStep =>
        wp.poll() match {
          case Some((ch, v)) =>
            // We got a value immediately
            val cont = wp.continuation(ch, v, ps)
            fastEval((wp.env, wp.lp, cont), stepsLeft - 1)
          
          case None =>
            // No value available, enqueue and schedule
            wp.channels.foreach(_.enqueue(wp))
            wp.channels.foreach(ps.smartEnqueue)
            wp.scheduleTimerIfNeeded(ps)
        }
    }
  }
}
