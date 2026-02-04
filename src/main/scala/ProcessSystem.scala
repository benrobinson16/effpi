// Effpi - verified message-passing programs in Dotty
// Copyright 2019 Alceste Scalas and Elias Benussi
// Released under the MIT License: https://opensource.org/licenses/MIT
package effpi.system

import java.util.concurrent.{Executors, ConcurrentHashMap}

import scala.util.{Failure, Success, Try}
import scala.concurrent.duration.Duration
import java.util.concurrent.{Executors, ConcurrentHashMap, ScheduledExecutorService, ScheduledFuture, TimeUnit}

import effpi.channel.{InChannel, OutChannel, ChannelStatus}

trait TimeoutDuration
case class AfterDelay(delayNanos: Long) extends TimeoutDuration
case class AtTime(timeNanos: Long) extends TimeoutDuration

trait ProcessSystem {

  import effpi.process.{ProcVar, Process, In}

  var alive = true
  val threads = scala.collection.mutable.ArrayBuffer.empty[Thread]

  def kill(): Unit = {
    alive = false
    threads.foreach { t => t.interrupt() }
    threads.foreach { t => t.join() }
    timerExecutor.shutdown()
  }

  val runningQueue = new SchedulingQueue[(Map[ProcVar[_], (_) => Process], List[() => Process], Process)]
  final def scheduleProc = runningQueue.enqueue
  final def consumeProc() = runningQueue.dequeue()

  val waitingQueue = new SchedulingQueue[InChannel[_]]
  final def scheduleInCh = waitingQueue.enqueue
  final def consumeInCh() = waitingQueue.dequeue()

  //TODO: below are the functions to be used with a state machine for inchannels
  final def smartEnqueue(inCh: InChannel[_]) = {
    val old = inCh.in.schedulingStatus.getAndSet(ChannelStatus.scheduled)
    if (old == ChannelStatus.unscheduled) scheduleInCh(inCh.in)
  }

  final def smartUnschedule(inCh: InChannel[_]) = {
    // IMPORTANT: use inCh.in, that is guaranteed to be an actual input chan
    val unscheduled = inCh.in.schedulingStatus.compareAndSet(
      ChannelStatus.running, ChannelStatus.unscheduled)
    if (!unscheduled) forceSchedule(inCh.in)
  }

  final def forceSchedule(inCh: InChannel[_]) = {
    // IMPORTANT: use inCh.in, that is guaranteed to be an actual input chan
    inCh.in.schedulingStatus.set(ChannelStatus.scheduled)
    scheduleInCh(inCh.in)
  }

  def init(threadsPerCore: Int): Unit

  def logQueuesStatus() = {
    var loggingTime = true
    while(loggingTime) {
      println(s"\nPending Processes queue size: ${runningQueue.size}")
      println(s"Waiting InChannels queue size: ${waitingQueue.size}\n")

      // This cannot be a Scala Try because it cannot deal with fatal exceptions
      // and InterruptedException is fatal
      try { Thread.sleep(1000) } catch {
        case e: InterruptedException =>
          // Thread.currentThread().interrupt()
          loggingTime = false
      }
    }
  }

  private lazy val timerExecutor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  private val pendingTimeouts = new ConcurrentHashMap[Long, ScheduledFuture[_]]()

  def scheduleTimeout(id: Long, timeout: TimeoutDuration, callback: () => Unit): Unit = {
    val delayNanos = timeout match {
      case AfterDelay(d) => d
      case AtTime(t) =>
        val now = System.nanoTime()
        math.max(t - now, 0)
    }

    val future = timerExecutor.schedule(
      new Runnable { def run() = callback() },
      delayNanos,
      TimeUnit.NANOSECONDS
    )

    // println(s"Scheduling timeout with delay: ${delayNanos}")

    pendingTimeouts.put(id, future)
  }

  def cancelTimeout(id: Long): Unit = {
    val future = pendingTimeouts.remove(id)
    if (future != null) {
      future.cancel(false)
    }
  }
}

object ProcessSystemRunnerImproved {

  def apply(threadsPerCore: Int = 1) = {
    val ps = new ProcessSystemRunnerImproved()
    ps.init(threadsPerCore)
    ps
  }

}

class ProcessSystemRunnerImproved extends ProcessSystem {

  override def init(threadsPerCore: Int): Unit = {
    val numCores = Runtime.getRuntime().availableProcessors()

    val numThreads = numCores * threadsPerCore

    (1 to numThreads).foreach { c =>
      threads += new Thread(new Executor(this))
    }

    (1 to numThreads).foreach { c =>
      threads += new Thread(new InputExecutor(this))
    }

    // threads += new Thread { override def run = logQueuesStatus() }

    threads.foreach{ t => t.start()}
  }
}

object ProcessSystemStateMachineMultiStep {

  def apply(threadsPerCore: Int = 1) = {
    val ps = new ProcessSystemStateMachineMultiStep()
    ps.init(threadsPerCore)
    ps
  }

}

class ProcessSystemStateMachineMultiStep extends ProcessSystem {

  override def init(threadsPerCore: Int): Unit = {


    val numCores = Runtime.getRuntime().availableProcessors()

    val numThreads = numCores * threadsPerCore

    (1 to numThreads).foreach { c =>
      threads += new Thread(new Executor(this))
    }

    (1 to numThreads).foreach { c =>
      threads += new Thread(new InputExecutor(this))
    }

    // threads += new Thread { override def run = logQueuesStatus() }

    threads.foreach{ t => t.start()}
  }
}
