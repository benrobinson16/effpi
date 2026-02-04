// Effpi - verified message-passing programs in Dotty
// Copyright 2019 Alceste Scalas and Elias Benussi
// Released under the MIT License: https://opensource.org/licenses/MIT
package effpi.process

import effpi.channel._
import effpi.system._
import scala.concurrent.duration.Duration
import scala.reflect.{ClassTag, TypeTest}
import scala.util.{Failure, Success, Try, NotGiven}
import scala.deriving.Mirror
import scala.compiletime.ops.int.*

// WARNING: double-check variance
sealed abstract class Process {
  def >>[P1 >: this.type <: Process, P2 <: Process](cont: => P2) = >>:[P1, P2](() => this, () => cont)

  def fork[P1 >: this.type <: Process] = Fork[P1](() => this)

  def spawn(ps: ProcessSystem) = {
    val self = this
    ps.scheduleProc((Map(), Nil, self))
  }
}
case class Out[C <: OutChannel[A], A](channel: C, v: A) extends Process

sealed abstract class TimeoutableProcess extends Process

// FIXME:
// * If we make In contravariant on A, type inference on "cont"
//   can push the domain to any, and therefore, we lose pattern matching
//   exhaustiveness checks --- e.g., on test2() below
//
// * If we make In invariant on A, we restore pattern matching exhaustiveness
//   checks on test2() below; however, we restrict subtyping, and we
//   sometimes need type annotations; see e.g. "cont" in test1a() and test1c()
//
// * If we make In covariant on A, we are wrong (and besides, we get a
//   variance error in the definition of In)
/** Receive a value from `channel`, and pass it to `cont`. */
case class In[C <: InChannel[A], A, P <: A => Process](channel: C, cont: P, timeout: Duration) extends TimeoutableProcess

case class Fork[P <: Process](p: () => P) extends Process

sealed abstract class PNil() extends Process

abstract class YieldCtx[-A](protected[effpi] val chan: OutChannel[A])
protected[effpi] class YieldCtxImpl[-A](chan: OutChannel[A]) extends YieldCtx[A](chan)
case class Yield[A](v: A)(implicit val ctx: Option[YieldCtx[A]]) extends Process

abstract class ProcVar[A](name: String)
case class ProcX[A]() extends ProcVar[A]("X")
case class ProcY[A]() extends ProcVar[A]("Y")
case class ProcZ[A]() extends ProcVar[A]("Z")

abstract class RecVar[A](name: String) extends ProcVar[A](name)

sealed abstract class RecX[A]() extends RecVar[A]("X")
case object RecX extends RecX[Unit]

sealed abstract class RecY[A]() extends RecVar[A]("Y")
case object RecY extends RecY[Unit]

sealed abstract class RecZ[A]() extends RecVar[A]("Z")
case object RecZ extends RecZ[Unit]

case class Def[V[X] <: ProcVar[X], A, P1 <: Process, P2 <: Process](name: V[A], pdef: A => P1, in: () => P2) extends Process
case class Call[V[X]  <: ProcVar[X], A](procvar: V[A], arg: A) extends Process

case class >>:[P1 <: Process, P2 <: Process](p1: () => P1, p2: () => P2) extends Process

// Extract the argument of a Function1 type
type ArgumentOf[F] = F match
  case Function1[i, ?] => i

// Wrapper that stores a continuation function along with runtime type information
case class MatchCase[A, B, P <: Process](cont: B => P, typeTest: TypeTest[A, B]) {
  // Try to match a value of type A against type B, and apply continuation if successful
  def tryMatch(value: A): Option[P] = {
    value match {
      case typeTest(v) => Some(cont(v))
      case _ => None
    }
  }
}

// Typeclass to automatically wrap a tuple of match functions with TypeTests
trait WrapMatches[A, Matches <: Tuple] {
  type Wrapped <: Tuple
  def wrap(matches: Matches): Wrapped
}

object WrapMatches {
  type Aux[A, Matches <: Tuple, W <: Tuple] = WrapMatches[A, Matches] { type Wrapped = W }
}

// Enforce constraints on the channels and match cases of a Branch
sealed trait ValidBranch[A, Chans <: Tuple, Matches <: Tuple]
object ValidBranch {
  given valid[A, Chans <: Tuple, Matches <: Tuple](using

    // The channels are all input channels accepting some subtype of A
    ev1: Tuple.Union[Chans] <:< InChannel[A],

    // The match cases are all functions to some process
    ev2: Tuple.Union[Matches] <:< Function1[?, Process],

    // The match cases cover exactly all possible inputs of A
    ev3: Tuple.Union[Tuple.Map[Matches, ArgumentOf]] =:= A,
    
  ): ValidBranch[A, Chans, Matches] with {}
}

case class CatchTimeout[P <: TimeoutableProcess, Q <: Process](p: () => P, onTimeout: () => Q) extends Process
case class EffpiTimeoutException(msg: String = "Timeout!") extends java.lang.RuntimeException(msg)

case class Branch[A, Chans <: Tuple, Matches <: Tuple]
  (channels: Chans, matches: Matches, timeout: Duration)
  (using val valid: ValidBranch[A, Chans, Matches], val wrapper: WrapMatches[A, Matches])
  extends TimeoutableProcess {

  // Lazily computed wrapped matches with runtime type information
  lazy val wrappedMatches: wrapper.Wrapped = wrapper.wrap(matches)

  // Find the first matching case for a received value
  // Returns the continuation process that matches the value's type
  def findMatch(value: A): Option[Process] = {
    def tryMatches(remaining: Tuple): Option[Process] = remaining match {
      case h *: tail =>
        h match {
          case mc: MatchCase[A, _, _] =>
            mc.tryMatch(value) match {
              case Some(p) => Some(p)
              case None => tryMatches(tail)
            }
          case _ => None
        }
      case EmptyTuple => None
    }
    tryMatches(wrappedMatches)
  }
}

// Helper for Branch - we can't write Branch[A, (InChannel[A]), ...] and
// it is awkward to define Branch[A, InChannel[A] *: EmptyTuple, ...]
// or Branch[A, Tuple1[InChannel[A]], ...], so make it easier by not expecting
// a tuple for a single channel.
type Branch1[A, Chan <: InChannel[A], Matches <: Tuple] =
  Branch[A, Tuple1[Chan], Matches]

package object dsl {

  /** Recursion: `P` loops on `V`, that represent a bound recursion variable. */
  type Rec[V[X] <: RecVar[X], P <: Process] = Def[V, Unit, P, P]

  /** Loop on a recursion variable `V`, expected to be bound by [[Rec]].*/
  type Loop[V[X] <: RecVar[X]] = Call[V, Unit]

  /** Execute `P1` and `P2` in parallel. */
  type Par[P1 <: Process, P2 <: Process] = Fork[P1] >>: P2

  /** Execute three processes in parallel. */
  type Par3[P1 <: Process, P2 <: Process, P3 <: Process] = Par[P1, Par[P2, P3]]

  /** Execute four processes in parallel. */
  type Par4[P1 <: Process, P2 <: Process, P3 <: Process, P4 <: Process] = (
    Par[P1, Par3[P2, P3, P4]]
  )

  /** Execute five processes in parallel. */
  type Par5[P1 <: Process, P2 <: Process, P3 <: Process, P4 <: Process,
            P5 <: Process] = (
    Par[P1, Par4[P2, P3, P4, P5]]
  )

  /** Execute six processes in parallel. */
  type Par6[P1 <: Process, P2 <: Process, P3 <: Process, P4 <: Process,
            P5 <: Process, P6 <: Process] = (
    Par[P1, Par5[P2, P3, P4, P5, P6]]
  )

  /** Execute seven processes in parallel. */
  type Par7[P1 <: Process, P2 <: Process, P3 <: Process, P4 <: Process,
            P5 <: Process, P6 <: Process, P7 <: Process] = (
    Par[P1, Par6[P2, P3, P4, P5, P6, P7]]
  )

  /** Execute eight processes in parallel. */
  type Par8[P1 <: Process, P2 <: Process, P3 <: Process, P4 <: Process,
            P5 <: Process, P6 <: Process, P7 <: Process, P8 <: Process] = (
    Par[P1, Par7[P2, P3, P4, P5, P6, P7, P8]]
  )

  /** Execute nine processes in parallel. */
  type Par9[P1 <: Process, P2 <: Process, P3 <: Process, P4 <: Process,
            P5 <: Process, P6 <: Process, P7 <: Process, P8 <: Process,
            P9 <: Process] = (
    Par[P1, Par8[P2, P3, P4, P5, P6, P7, P8, P9]]
  )

  /** Execute ten processes in parallel. */
  type Par10[P1 <: Process, P2 <: Process, P3 <: Process, P4 <: Process,
             P5 <: Process, P6 <: Process, P7 <: Process, P8 <: Process,
             P9 <: Process, P10 <: Process] = (
    Par[P1, Par9[P2, P3, P4, P5, P6, P7, P8, P9, P10]]
  )

  /** Execute 11 processes in parallel. */
  type Par11[P1 <: Process, P2 <: Process,  P3 <: Process, P4 <: Process,
             P5 <: Process, P6 <: Process,  P7 <: Process, P8 <: Process,
             P9 <: Process, P10 <: Process, P11 <: Process] = (
    Par[P1, Par10[P2, P3, P4, P5, P6, P7, P8, P9, P10, P11]]
  )

  /** Execute 12 processes in parallel. */
  type Par12[P1 <: Process, P2 <: Process,  P3 <: Process,  P4 <: Process,
             P5 <: Process, P6 <: Process,  P7 <: Process,  P8 <: Process,
             P9 <: Process, P10 <: Process, P11 <: Process, P12 <: Process] = (
    Par[P1, Par11[P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12]]
  )

  /** Execute 13 processes in parallel. */
  type Par13[P1 <: Process,  P2 <: Process,  P3 <: Process,  P4 <: Process,
             P5 <: Process,  P6 <: Process,  P7 <: Process,  P8 <: Process,
             P9 <: Process,  P10 <: Process, P11 <: Process, P12 <: Process,
             P13 <: Process] = (
    Par[P1, Par12[P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13]]
  )

  /** Execute 14 processes in parallel. */
  type Par14[P1 <: Process,  P2 <: Process,  P3 <: Process,  P4 <: Process,
             P5 <: Process,  P6 <: Process,  P7 <: Process,  P8 <: Process,
             P9 <: Process,  P10 <: Process, P11 <: Process, P12 <: Process,
             P13 <: Process, P14 <: Process] = (
    Par[P1, Par13[P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14]]
  )

  /** Execute `P` so that it can yield a value of type `A` to the caller.
  *
  * This is an experimental type, with the goal of modelling the
  * [[https://doc.akka.io/docs/akka/2.5/actors.html#ask-send-and-receive-future "ask pattern"]]. */
  type Yielding[A, P <: Process] = YieldCtx[A] ?=> P

  /** Do nothing (inactive process). */
  case object nil extends PNil

  /** Send argument `v` via channel `c`. */
  def send[C <: OutChannel[A], A](c: C, v: A) = Out[C,A](c, v)

  /** Use channel `c` to receive a value, then pass it to the `cont`inuation. */
  def receive[C <: InChannel[A], A, P <: A => Process](c: C)(cont: P)(implicit timeout: Duration) = In[C,A,P](c, cont, timeout)

  def catchTimeout[P <: TimeoutableProcess, Q <: Process](p: => P, q: => Q) =
    CatchTimeout[P, Q](() => p, () => q)

  def branch[A, Chans <: Tuple, Matches <: Tuple](
    channels: Chans, matches: Matches
  )(using ValidBranch[A, Chans, Matches], WrapMatches[A, Matches])(implicit timeout: Duration) =
    Branch[A, Chans, Matches](channels, matches, timeout)

  // Helper for branch, when there is only a single channel
  def branch1[A, Chan <: InChannel[A], Matches <: Tuple](
    channel: Chan, matches: Matches
  )(using ValidBranch[A, Tuple1[Chan], Matches], WrapMatches[A, Matches])(implicit timeout: Duration) =
    Branch[A, Tuple1[Chan], Matches](Tuple1(channel), matches, timeout)
  
  /** Fork `p` as a separate process.
  *
  * NOTE: in practice, you might probably want to use [[par]].
  */
  def fork[P <: Process](p: => P) = Fork[P](() => p)

  /** Execute two processes in parallel. */
  def par[P1 <: Process,
          P2 <: Process](p1: => P1, p2: => P2): Par[P1, P2] = {
    Fork[P1](() => p1) >> p2
  }

  /** Execute three processes in parallel. */
  def par[P1 <: Process,
          P2 <: Process,
          P3 <: Process](p1: => P1,
                         p2: => P2,
                         p3: => P3): Par3[P1, P2, P3] = {
    par(p1, par(p2, p3))
  }

  /** Execute four processes in parallel. */
  def par[P1 <: Process,
          P2 <: Process,
          P3 <: Process,
          P4 <: Process](p1: => P1,
                         p2: => P2,
                         p3: => P3,
                         p4: => P4): Par4[P1, P2, P3, P4] = {
    par(p1, par(p2, p3, p4))
  }

  /** Execute five processes in parallel. */
  def par[P1 <: Process,
          P2 <: Process,
          P3 <: Process,
          P4 <: Process,
          P5 <: Process](p1: => P1,
                         p2: => P2,
                         p3: => P3,
                         p4: => P4,
                         p5: => P5): Par5[P1, P2, P3, P4, P5] = {
    par(p1, par(p2, p3, p4, p5))
  }

  /** Execute six processes in parallel. */
  def par[P1 <: Process,
          P2 <: Process,
          P3 <: Process,
          P4 <: Process,
          P5 <: Process,
          P6 <: Process](p1: => P1,
                         p2: => P2,
                         p3: => P3,
                         p4: => P4,
                         p5: => P5,
                         p6: => P6): Par6[P1, P2, P3, P4, P5, P6] = {
    par(p1, par(p2, p3, p4, p5, p6))
  }

  /** Execute seven processes in parallel. */
  def par[P1 <: Process,
          P2 <: Process,
          P3 <: Process,
          P4 <: Process,
          P5 <: Process,
          P6 <: Process,
          P7 <: Process](p1: => P1,
                         p2: => P2,
                         p3: => P3,
                         p4: => P4,
                         p5: => P5,
                         p6: => P6,
                         p7: => P7): Par7[P1, P2, P3, P4, P5, P6, P7] = {
    par(p1, par(p2, p3, p4, p5, p6, p7))
  }

  /** Execute eight processes in parallel. */
  def par[P1 <: Process,
          P2 <: Process,
          P3 <: Process,
          P4 <: Process,
          P5 <: Process,
          P6 <: Process,
          P7 <: Process,
          P8 <: Process](p1: => P1,
                         p2: => P2,
                         p3: => P3,
                         p4: => P4,
                         p5: => P5,
                         p6: => P6,
                         p7: => P7,
                         p8: => P8): Par8[P1, P2, P3, P4, P5, P6, P7, P8] = {
    par(p1, par(p2, p3, p4, p5, p6, p7, p8))
  }

  /** Yield a value `v` to the caller of the present process.
  *
  * This is an experimental method, that can only be invoked within a process
  * typed by [[Yielding]]. Its goal is to capture the
  * [[https://doc.akka.io/docs/akka/2.5/actors.html#ask-send-and-receive-future "ask pattern"]]. */
  def pyield[A](v: A): YieldCtx[A] ?=> Yield[A] = {
    Yield[A](v)(Some(implicitly[YieldCtx[A]]))
  }

  def pdef[V[X] <: ProcVar[X], A, P1 <: Process, P2 <: Process](name: V[A])(pdef: A => P1)(in: => P2) = Def[V, A, P1, P2](name, pdef, () => in)

  def pcall[V[X] <: ProcVar[X], A](name: V[A], arg: A) = Call[V,A](name, arg)

  def rec[V[X] <: RecVar[X], P <: Process](v: V[Unit])(p: => P): Rec[V, P] = Def[V, Unit, P, P](v, (x) => p, () => p)

  def loop[V[X] <: RecVar[X]](v: V[Unit]): Loop[V] = Call[V, Unit](v, ())

  def eval(p: Process): Try[Unit] = Try(eval(Map(), Nil, p))

  @annotation.tailrec
  def eval(env: Map[ProcVar[_], (_) => Process], lp: List[() => Process],
           p: Process, timeoutContinuation: Option[() => Process] = None): Unit = p match {
    case i: In[_,_,_] => {
      val ic = i.channel

      var v: Option[Any] = None
      try {
        v = Some(ic.receive()(i.timeout))
      } catch {
        case e: EffpiTimeoutException => {
          if (timeoutContinuation.isEmpty) {
            // Only rethrow if no timeout continuation is defined
            throw e
          }
        }
      }

      if (v == None && timeoutContinuation.isDefined) {
        val cont = timeoutContinuation.get
        eval(env, lp, cont())
      } else {
        val cont = if (ic.synchronous) {
          // We received a tuple containing a value and an ack channel
          val (v2, ack) = v.get.asInstanceOf[Tuple2[Any, OutChannel[Unit]]]
          ack.send(())
          i.cont.asInstanceOf[Any => Process](v2)
        } else {
          i.cont.asInstanceOf[Any => Process](v.get)
        }
        eval(env, lp, cont)
      }
    }
    case wt: CatchTimeout[_, _] => {
      eval(env, lp, wt.p(), timeoutContinuation=Some(wt.onTimeout))
    }
    case b: Branch[_, _, _] => {
      val deadline = if (b.timeout.isFinite) Some(System.nanoTime() + b.timeout.toNanos) else None

      // Keep polling all channels until we get a message or timeout
      @annotation.tailrec
      def pollUntilTimeout(): Option[(Any, Boolean)] = {
        if (deadline.isDefined && System.nanoTime() >= deadline.get) {
          None
        } else {
          // Shuffle the channels
          var shuffledChannels = scala.util.Random.shuffle(b.channels.toList)
          var result: Option[(Any, Boolean)] = None

          while (!shuffledChannels.isEmpty && result == None) {
            val channel = shuffledChannels.head
            shuffledChannels = shuffledChannels.tail

            val ic = channel.asInstanceOf[InChannel[Any]]
            ic.poll() match {
              case Some(res) => result = Some((res, ic.synchronous))
              case None => ()
            }
          }

          result match {
            case Some(res) => Some(res)
            case None =>
              // No message yet, yield and retry
              Thread.`yield`()
              pollUntilTimeout()
          }
        }
      }

      val pollResult = pollUntilTimeout()

      if (pollResult.isEmpty) {
        if (timeoutContinuation.isDefined) {
        val cont = timeoutContinuation.get
        eval(env, lp, cont())
        } else {
          throw EffpiTimeoutException(
            "Branch: No message received from any channel within timeout"
          )
        }
      } else {
        val (rawValue, isSynchronous) = pollResult.get

        // Handle synchronous channels
        val actualValue = if (isSynchronous) {
          val (v2, ack) = rawValue.asInstanceOf[Tuple2[Any, OutChannel[Unit]]]
          ack.send(())
          v2
        } else {
          rawValue
        }

        // Find the matching continuation based on the value's type
        // Cast to the existential type A from Branch[A, _, _]
        b.asInstanceOf[Branch[Any, _, _]].findMatch(actualValue) match {
          case Some(cont) => eval(env, lp, cont)
          case None =>
            throw new RuntimeException(s"Branch: No matching case for received value: $actualValue")
        }
      }
    }
    case o: Out[_,_] => {
      val oc = o.channel.asInstanceOf[OutChannel[Any]]
      if (oc.synchronous) {
        // Send an ack channel together with the value
        val ack = oc.create[Unit](false) // The ack channel *must* be async
        oc.send((o.v, ack.out))
        // FIXME: allow to specify timeouts
        ack.in.receive()(concurrent.duration.Duration.Inf)
      } else {
        oc.send(o.v)
      }
      lp match {
        case Nil => ()
        case lh :: lt => eval(env, lt, lh())
      }
    }
    case f: Fork[_] => {
      new Thread { override def run = eval(env, Nil, f.p()) }.start()
      lp match {
        case Nil => ()
        case lh :: lt => eval(env, lt, lh())
      }
    }
    case n: PNil => lp match {
      case Nil => ()
      case lh :: lt => eval(env, lt, lh())
    }
    case y: Yield[_] => {
      y.ctx match {
        case Some(c) => c.chan.asInstanceOf[OutChannel[Any]].send(y.v)
        case None => ()
      }
      lp match {
        case Nil => ()
        case lh :: lt => eval(env, lt, lh())
      }
    }
    case d: Def[_,_,_,_] => {
      eval(env + (d.name -> d.pdef), lp, d.in())
    }
    case c: Call[_,_] => {
      env.get(c.procvar) match {
        case Some(p) => {
          // println(s"*** Calling ${c.procvar}(${c.arg})")
          eval(env, lp, p.asInstanceOf[Any => Process](c.arg))
        }
        case None => {
          throw new RuntimeException(s"Unbound process variable: ${c.procvar}")
        }
      }
    }
    case s: >>:[_,_] => {
      eval(env, s.p2 :: lp, s.p1())
    }
  }
}
