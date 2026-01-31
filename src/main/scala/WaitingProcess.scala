package effpi.waiting

import java.util.concurrent.atomic.AtomicInteger

import effpi.process._
import effpi.channel._
import effpi.system._

enum WaitingState(val value: Int) {
  case Pending extends WaitingState(0)
  case Claiming extends WaitingState(1)
  case Resolved extends WaitingState(2)
}

class AtomicState(initial: WaitingState) {
  private val atomicInt = new AtomicInteger(initial.value)

  def get(): WaitingState = {
    val value = atomicInt.get()
    WaitingState.values.find(_.value == value).get
  }

  def set(newValue: WaitingState): Unit = {
    atomicInt.set(newValue.value)
  }

  def compareAndSet(expected: WaitingState, newValue: WaitingState): Boolean = {
    atomicInt.compareAndSet(expected.value, newValue.value)
  }
}

class WaitingProcess(val env: Map[ProcVar[_], (_) => Process],
                     val lp: List[() => Process],
                     val proc: TimeoutableProcess,
                     val deadline: Option[Long],
                     val onTimeout: Option[() => Process]) {

  // A unique ID for this waiting process.
  // Use to cancel waiting timeouts, etc.
  val id: Long = System.nanoTime() ^ Thread.currentThread().getId()

  val channels = proc match {
    case i: In[_, _, _] => List(i.channel.asInstanceOf[InChannel[Any]])
    case b: Branch[_, _, _] => b.channels.toList.map(_.asInstanceOf[InChannel[Any]])
  }

  // MARK: - State management

  // State of the waiting process.
  // Pending = waiting
  // Claiming = being claimed by a thread, but not yet resolved
  //    The claiming thread may not be able to resolve it, e.g. if
  //    no messages are available.
  // Resolved = already claimed and resolved
  private val waitingState: AtomicState = new AtomicState(WaitingState.Pending)

  def state: WaitingState = waitingState.get()

  def tryClaim(): Boolean = {
    waitingState.compareAndSet(WaitingState.Pending, WaitingState.Claiming)
  }

  def markResolved() = waitingState.set(WaitingState.Resolved)
  def markPending() = waitingState.set(WaitingState.Pending)

  // MARK: - Timeout handling

  def deadlineExpired: Boolean = {
    deadline match {
      case Some(d) if System.nanoTime() >= d => true
      case _ => false
    }
  }

  def scheduleTimerIfNeeded(ps: ProcessSystem): Unit = {
    deadline.foreach { dl =>
      ps.scheduleTimeout(id, AtTime(dl), makeTimeoutCallback(ps))
    }
  }

  def makeTimeoutCallback(ps: ProcessSystem): () => Unit = {
    def callback(): Unit = {
      state match {
        case WaitingState.Resolved =>
          // The timer went off, but the op was already handled
          // Do nothing
          ()

        case WaitingState.Pending =>
          if (tryClaim()) {
            // We won the ability to handle the op
            markResolved()
            channels.foreach(_.removeWaitingProcById(id))
            onTimeout match {
              case Some(handler) => ps.scheduleProc((env, lp, handler()))
              case None => throw new RuntimeException("Timeout occurred but no timeout handler defined.")
            }
          } else {
            // Another thread is trying to handle the op
            // Schedule a retry shortly
            ps.scheduleTimeout(id, AfterDelay(100_000), () => callback())
          }

        case WaitingState.Claiming =>
          // Another thread is trying to handle the op
          // Schedule a retry shortly
          ps.scheduleTimeout(id, AfterDelay(100_000), () => callback())
      }
    }

    callback
  }

  // MARK: - Receiving handling

  private def extractReceivedValue(ch: InChannel[Any], value: Any, ps: ProcessSystem): Any = {
    if (ch.synchronous) {
      // Received ack channel, send the ack back
      val (v2, ack) = value.asInstanceOf[Tuple2[Any, OutChannel[Unit]]]
      ack.send(())
  
      // Wake the ack recipient (depends on process system type)
      ps match {
        case _: ProcessSystemRunnerImproved => ps.scheduleInCh(ack.dualIn)
        case _: ProcessSystemStateMachineMultiStep => ps.smartEnqueue(ack.dualIn)
      }
  
      v2
    } else {
      value
    }
  }

  def continuation(ch: InChannel[Any], value: Any, ps: ProcessSystem): Process = {
    val actualValue = extractReceivedValue(ch, value, ps)

    proc match {
      case i: In[_, _, _] =>
        i.cont.asInstanceOf[Any => Process](actualValue)

      case b: Branch[_, _, _] =>
        b.asInstanceOf[Branch[Any, _, _]].findMatch(actualValue) match {
          case Some(cont) => cont
          case None => throw new RuntimeException(s"Branch: no matching case for value $actualValue")
        }
    }
  }

  def poll(): Option[(InChannel[Any], Any)] = {
    if (channels.isEmpty) {
      // No channels
      None
    } else if (channels.length == 1) {
      // 1 channel: either an In or Branch1
      val ch = channels.head
      ch.poll().map(v => (ch, v))
    } else {
      // Multiple channels: shuffle for fairness
      val shuffled = scala.util.Random.shuffle(channels)
      var result: Option[(InChannel[Any], Any)] = None
      var remaining = shuffled
      while (result.isEmpty && remaining.nonEmpty) {
        val ch = remaining.head
        remaining = remaining.tail
        ch.poll() match {
          case Some(v) => result = Some((ch, v))
          case None => ()
        }
      }
      result
    }
  }
}