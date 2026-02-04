package effpi.examples.raft

import effpi.channel.{Channel => Chan, InChannel => IChan, OutChannel => OChan}
import effpi.process._
import effpi.process.dsl._
import effpi.process.consWrap
import effpi.process.given
import effpi.verifier.verify
import scala.concurrent.duration.Duration

// Logging utility for formatted output
object Logger {
 private val startTime = System.currentTimeMillis()

 def log(server: Int, term: Int, role: String, event: String): Unit = {
   val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
   val formattedTime = f"$elapsed%6.2f"
   val formattedServer = f"$server%3d"
   val formattedTerm = f"$term%4d"
   val formattedRole = f"$role%-10s"

   println(s"[$formattedTime s] Server $formattedServer | Term $formattedTerm | $formattedRole | $event")
 }
}

type RPC = RequestVote[_] | AppendEntries[_]
case class RequestVote[T <: OChan[VoteResponse]](reply: T, from: Int, t: Int, upToDate: Boolean)
case class AppendEntries[T <: OChan[AckAppendEntries]](reply: T, from: Int, t: Int)

type VoteResponse = GrantVote | RefuseVote
case class GrantVote(t: Int)
case class RefuseVote(t: Int)

type AckAppendEntries = AcceptAppendEntries | RefuseAppendEntries
case class AcceptAppendEntries(t: Int)
case class RefuseAppendEntries(t: Int)

case class TimerExpired()
case class TimerReset(min: Int, max: Int)
val electionTimeout = TimerReset(10_000, 15_000)
val heartbeatTimeout = TimerReset(8_000, 8_001)

case class Peer(rv: IChan[RequestVote[_]], ae: IChan[AppendEntries[_]])

private implicit val timeout: Duration = Duration(1000, "seconds")

sealed abstract class RecFollower[A]() extends RecVar[A]("Follower")
case object RecFollower extends RecFollower[Unit]
sealed abstract class RecElection[A]() extends RecVar[A]("Election")
case object RecElection extends RecElection[Unit]
sealed abstract class RecCandidate[A]() extends RecVar[A]("Candidate")
case object RecCandidate extends RecCandidate[Unit]
sealed abstract class RecLeader[A]() extends RecVar[A]("Leader")
case object RecLeader extends RecLeader[Unit]

class Server[Inbox <: Chan[RPC], Peer1 <: OChan[RPC], Peer2 <: OChan[RPC]](me: Int, val inbox: Inbox, val peer1: Peer1, val peer2: Peer2) {
  val timerReset = Chan.async[TimerReset]()
  val timeoutChan = Chan[TimerExpired]()
  val majority = 2
  var timerId = 0
  var t = 0
  var vote: Option[Int] = None

  // MARK: - Helper protocols

  type Broadcast[A <: RPC] = Par[Out[peer1.type, A], Out[peer2.type, A]]

  type Timer = Rec[RecX,
    In[timerReset.type, TimerReset, TimerReset => 
      Rec[RecY,
        CatchTimeout[
          // If we receive a timer reset before timeout, we loop to RecY
          // because we still have a pending timeout.
          In[timerReset.type, TimerReset, TimerReset => Loop[RecY]],

          // Once we timeout, we loop to RecX because we now have no
          // pending timeout. The node must reset the timer to incur
          // a new future timeout event.
          Out[timeoutChan.type, TimerExpired] >>: Loop[RecX]
        ]
      ]
    ]
  ]

  // MARK: - Response behaviours

  type GrantVoteBehaviour[ReplyC <: OChan[VoteResponse]] = 
    (Out[ReplyC, GrantVote]
      >>: Out[timerReset.type, TimerReset])
      >>: Loop[RecFollower]

  type RefuseVoteBehaviour[ReplyC <: OChan[VoteResponse], V[A] <: RecVar[A]] =
    Out[ReplyC, RefuseVote]
      >>: Loop[V]

  type VoteResponseBehaviour[ReplyC <: OChan[VoteResponse], V[A] <: RecVar[A]] =
    GrantVoteBehaviour[ReplyC] | RefuseVoteBehaviour[ReplyC, V]

  type AcceptAppendEntriesBehaviour[ReplyC <: OChan[AckAppendEntries]] = 
    (Out[ReplyC, AcceptAppendEntries]
      >>: Out[timerReset.type, TimerReset])
      >>: Loop[RecFollower]

  type RefuseAppendEntriesBehaviour[ReplyC <: OChan[AckAppendEntries], V[A] <: RecVar[A]] = 
    Out[ReplyC, RefuseAppendEntries]
      >>: Loop[V]

  type AppendEntriesResponseBehaviour[ReplyC <: OChan[AckAppendEntries], V[A] <: RecVar[A]] =
    AcceptAppendEntriesBehaviour[ReplyC] | RefuseAppendEntriesBehaviour[ReplyC, V]

  // MARK: - State behaviours

  type Follower = Rec[RecFollower,
    Branch[RPC | TimerExpired, (inbox.type, timeoutChan.type), (
      (rv: RequestVote[_]) => VoteResponseBehaviour[rv.reply.type, RecFollower],
      (ae: AppendEntries[_]) => AppendEntriesResponseBehaviour[ae.reply.type, RecFollower],
      TimerExpired => Candidate
    )]
  ]

  type CandidateElection[ReplyC <: Chan[VoteResponse]] =
    Par[
      Broadcast[RequestVote[ReplyC]],
      Rec[RecCandidate,
        Branch[RPC | TimerExpired | VoteResponse, (inbox.type, timeoutChan.type, ReplyC), (
          (rv: RequestVote[_]) => VoteResponseBehaviour[rv.reply.type, RecCandidate],
          (ae: AppendEntries[_]) => AppendEntriesResponseBehaviour[ae.reply.type, RecCandidate],
          TimerExpired => Loop[RecElection],
          GrantVote => Leader | (Out[timerReset.type, TimerReset] >>: Loop[RecFollower]) | Loop[RecCandidate],
          RefuseVote => Loop[RecCandidate]
        )]
      ]
    ]

  type Candidate = Rec[RecElection,
    Out[timerReset.type, TimerReset]
      >>: CandidateElection[Chan[VoteResponse]]
  ]

  type Leader =
   Out[timerReset.type, TimerReset]
     >>: Par[
       Broadcast[AppendEntries[Chan[AckAppendEntries]]],
       Rec[RecLeader,
        Branch[RPC | TimerExpired, (inbox.type, timeoutChan.type), (
          (ae: AppendEntries[_]) => AppendEntriesResponseBehaviour[ae.reply.type, RecLeader],
          (rv: RequestVote[_]) => VoteResponseBehaviour[rv.reply.type, RecLeader],
          TimerExpired => (Out[timerReset.type, TimerReset] >>: (Loop[RecLeader] | Par[Broadcast[AppendEntries[Chan[AckAppendEntries]]], Loop[RecLeader]]))
        )]
    ]
  ]

  type ServerBehaviour = Par3[
    Out[timerReset.type, TimerReset],
    Timer,
    Follower
  ]

  // MARK: - Helper implementations

  def broadcast[A <: RPC](msg: A): Broadcast[A] = {
    par(send(peer1, msg), send(peer2, msg))
  }

  def timer: Timer = {
    rec(RecX) {
      receive(timerReset) { tr =>
        var millis = tr.min + scala.util.Random.nextInt(tr.max - tr.min)

        rec(RecY) {
          catchTimeout(
            {
              // Set the timeout for the receive operation
              implicit val timeout = Duration(millis.toLong, "millis")
              receive(timerReset) { newTr =>
                millis = newTr.min + scala.util.Random.nextInt(newTr.max - newTr.min)
                loop(RecY)
              }
            },
            // Timer expires
            send(timeoutChan, TimerExpired()) >> loop(RecX)
          )
        }  
      }
    }
  }

  // MARK: - State implementations

  def follower: Follower = {
    Logger.log(me, t, "FOLLOWER", "Starting as follower")
    rec(RecFollower) {
      branch((inbox, timeoutChan), (
        (rv: RequestVote[_]) => {
          def deny: RefuseVoteBehaviour[rv.reply.type, RecFollower] = {
            Logger.log(me, t, "FOLLOWER", s"Denying vote to Server ${rv.from}")
            send(rv.reply, RefuseVote(t))
              >> loop(RecFollower)
          }

          def grant: GrantVoteBehaviour[rv.reply.type] = {
            t = Math.max(t, rv.t)
            vote = Some(rv.from)
            Logger.log(me, t, "FOLLOWER", s"Granting vote to Server ${rv.from}")

            send(rv.reply, GrantVote(rv.t))
              >> send(timerReset, electionTimeout)
              >> loop(RecFollower)
          }

          if (rv.t < t) deny
          else if (rv.t == t) {
            if ((vote.isEmpty || vote.contains(rv.from)) && rv.upToDate) grant
            else deny
          }
          else grant
        },

        (ae: AppendEntries[_]) => {
          Logger.log(me, t, "FOLLOWER", s"Received AppendEntries from Server ${ae.from}")

          def failed: RefuseAppendEntriesBehaviour[ae.reply.type, RecFollower] = {
            send(ae.reply, RefuseAppendEntries(t))
              >> loop(RecFollower)
          }

          def success: AcceptAppendEntriesBehaviour[ae.reply.type] = {
            t = Math.max(t, ae.t)
            send(ae.reply, AcceptAppendEntries(ae.t))
              >> send(timerReset, electionTimeout)
              >> loop(RecFollower)
          }

          if (ae.t < t) failed
          else if (ae.t == t) success
          else { vote = None; success }
        },

        (x: TimerExpired) => {
          Logger.log(me, t, "FOLLOWER", "Election timeout expired - transitioning to CANDIDATE")
          candidate
        }
      ))
    }
  }

  def candidateElection[T <: Chan[VoteResponse]](voteResponseChan: T): CandidateElection[T] = {
    var votesRemaining = majority - 1

    par(
      broadcast(RequestVote[T](voteResponseChan, me, t, true)),
      rec(RecCandidate) {

        branch((inbox, timeoutChan, voteResponseChan), (
          (rv: RequestVote[_]) => {
            if (rv.t > t) {
              Logger.log(me, t, "CANDIDATE", s"Received RequestVote[_] from Server ${rv.from} (term=${rv.t} > $t) - reverting to FOLLOWER")

              t = rv.t
              vote = Some(rv.from)

              send(rv.reply, GrantVote(rv.t))
                >> send(timerReset, electionTimeout)
                >> loop(RecFollower)
            } else {
              send(rv.reply, RefuseVote(t))
                >> loop(RecCandidate)
            }
          },
          (ae: AppendEntries[_]) => {
            if (ae.t >= t) {
              t = ae.t
              vote = None
    
              send(ae.reply, AcceptAppendEntries(ae.t))
                >> send(timerReset, electionTimeout)
                >> loop (RecFollower)
            } else {
              send(ae.reply, RefuseAppendEntries(t))
                >> loop (RecCandidate)
            }
          },
          TimerExpired => {
              Logger.log(me, t, "CANDIDATE", s"Election timeout - starting new election")
              loop(RecElection)
          },
          (gv: GrantVote) => {
            if (gv.t == t) {
              votesRemaining -= 1
              
              if (votesRemaining == 0) {
                Logger.log(me, t, "CANDIDATE", s"Election win! Becoming LEADER")
                leader
              } else {
                loop(RecCandidate)
              }
            } else if (gv.t > t) {
              send(timerReset, electionTimeout)
                >> loop(RecFollower)
            } else {
              // Ignore votes for earlier terms
              loop(RecCandidate)
            }
          },
          RefuseVote => loop(RecCandidate)
        ))
      }
    )
  }

  def candidate: Candidate = {
    rec(RecElection) {
      t += 1
      val voteResponseChan = Chan.async[VoteResponse]()

      send(timerReset, electionTimeout)
        >> candidateElection(voteResponseChan)
    }
  }

  def leader: Leader = {
    val appendEntriesReplyChan = Chan.async[AckAppendEntries]()

    Logger.log(me, t, "LEADER", "Became leader - sending initial heartbeat")
    send(timerReset, heartbeatTimeout)
      >> par(
        broadcast(AppendEntries(appendEntriesReplyChan, me, t)),
        rec(RecLeader) {
          branch((inbox, timeoutChan), (
            (ae: AppendEntries[_]) => {
              if (ae.t >= t) {
                Logger.log(me, t, "LEADER", s"Received AppendEntries from Server ${ae.from} (term=${ae.t}) - reverting to FOLLOWER")

                t = ae.t
                vote = None

                send(ae.reply, AcceptAppendEntries(ae.t))
                  >> send(timerReset, electionTimeout)
                  >> loop(RecFollower)
              } else {
                send(ae.reply, RefuseAppendEntries(t))
                  >> loop(RecLeader)
              }
            },
            (rv: RequestVote[_]) => {
              if (rv.t > t) {
                Logger.log(me, t, "LEADER", s"Received RequestVote[_] from Server ${rv.from} (term=${rv.t} > $t) - reverting to FOLLOWER")
    
                t = rv.t
                vote = Some(rv.from)
    
                send(rv.reply, GrantVote(t))
                  >> send(timerReset, electionTimeout)
                  >> loop(RecFollower)
              } else {
                send(rv.reply, RefuseVote(t))
                  >> loop(RecLeader)
              }
            },
            TimerExpired => {
                Logger.log(me, t, "LEADER", "Heartbeat timeout - sending AppendEntries to all followers")

                if (scala.util.Random.nextInt(10) >= 8) {
                  Logger.log(me, t, "LEADER", "...Simulating missed heartbeat")
                  send(timerReset, heartbeatTimeout)
                    >> loop(RecLeader)
                } else {
                  send(timerReset, heartbeatTimeout)
                    >> par(broadcast(AppendEntries(appendEntriesReplyChan, me, t)), loop(RecLeader))
                }
            }
          ))
        }
      )
  }

  // Create a process that runs the server: timer + follower state
  def run(): ServerBehaviour = {
    // Start the timer with an initial reset
    // Run timer and follower in parallel
    par(send(timerReset, electionTimeout), timer, follower)
  }
}

// sbt "examples/runMain effpi.examples.Raft.Main"
object Main {
  def main(): Unit = main(Array())

  def system() = {
    // Create inbox channels for each server
    val inbox0 = Chan.async[RPC]()
    val inbox1 = Chan.async[RPC]()
    val inbox2 = Chan.async[RPC]()

    // Create the three servers
    val server0 = new Server(0, inbox0, inbox1, inbox2)
    val server1 = new Server(1, inbox1, inbox0, inbox2)
    val server2 = new Server(2, inbox2, inbox0, inbox1)

    // Spawn all three servers
    par(server0.run(), server1.run(), server2.run())
  }

  def main(args: Array[String]): Unit = {
    // Basic eval strategy
    // eval(system())

    // ProcessSystemRunner strategy
    implicit val ps = effpi.system.ProcessSystemRunnerImproved()
    system().spawn(ps)
    Thread.sleep(120000); ps.kill()
    println("*** ProcessSystem killed after 120 seconds.")
  }
}
