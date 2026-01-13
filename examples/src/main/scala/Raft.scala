package effpi.examples.raft

import effpi.channel.{Channel => Chan, InChannel => IChan, OutChannel => OChan}
import effpi.process._
import effpi.process.dsl._
import effpi.process.consWrap
import effpi.process.given
import effpi.verifier.verify
import scala.concurrent.duration.Duration

// sbt "examples/runMain Raft2.scala"
//  sbt "examples/runMain effpi.examples.Raft.Main"

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

case class RequestVote(reply: OChan[VoteResponse], from: Int, t1: Int, upToDate: Boolean)

case class GrantVote(t1: Int)
case class RefuseVote(t1: Int)

case class AppendEntries(reply: OChan[AckAppendEntries], from: Int, t1: Int)

case class AcceptAppendEntries(t1: Int)
case class RefuseAppendEntries(t1: Int)

case class TimerExpired()

type RpcMessage = RequestVote | AppendEntries
type VoteResponse = GrantVote | RefuseVote
type AckAppendEntries = AcceptAppendEntries | RefuseAppendEntries

case class ElectionWin()
case class ElectionLoss()
case class ElectionInvalid()
type ElectionResult = ElectionWin | ElectionLoss | ElectionInvalid

case class TimerReset(min: Int, max: Int)
val electionTimeout = TimerReset(10_000, 15_000)
val candidateTimeout = TimerReset(5_000, 5_001) // FIXME This should be electionTimeout, but faster for detecting win
val heartbeatTimeout = TimerReset(8_000, 8_001)

case class Peer(rv: IChan[RequestVote], ae: IChan[AppendEntries])

// This is the implicit timeout that gets passed to every receive(...) { ... }
// FIXME Does this make sense for Raft? What happens when a timeout occurs?
// FIXME Could we use this for handling no heartbeats being received?
private implicit val timeout: Duration = Duration(1000, "seconds")

sealed abstract class RecFollower[A]() extends RecVar[A]("Follower")
case object RecFollower extends RecFollower[Unit]

sealed abstract class RecElection[A]() extends RecVar[A]("Election")
case object RecElection extends RecElection[Unit]

sealed abstract class RecCandidate[A]() extends RecVar[A]("Candidate")
case object RecCandidate extends RecCandidate[Unit]

sealed abstract class RecLeader[A]() extends RecVar[A]("Leader")
case object RecLeader extends RecLeader[Unit]

class Server[Inbox <: Chan[RpcMessage], Peer1 <: OChan[RpcMessage], Peer2 <: OChan[RpcMessage]](me: Int, val inbox: Inbox, val peer1: Peer1, val peer2: Peer2) {
 val timerReset = Chan.async[TimerReset]()
 val timeoutChan = Chan[TimerExpired]()
 val majority = 2
 var timerId = 0
 var t = 0
 var vote: Option[Int] = None

 type Broadcast[A <: RpcMessage] = Par[Out[peer1.type, A], Out[peer2.type, A]]
 type TimerWait = Out[timeoutChan.type, TimerExpired] | PNil
 type Timer = Rec[RecX, In[timerReset.type, TimerReset, TimerReset => Par[TimerWait, Loop[RecX]]]]

 // FIXME Reply channels - need to pass these in, after matching against RpcMessage and extracting
 type GrantVoteBehaviour = (Out[OChan[VoteResponse], GrantVote] >>: Out[timerReset.type, TimerReset]) >>: Loop[RecFollower]
 type RefuseVoteBehaviour[V[A] <: RecVar[A]] = Out[OChan[VoteResponse], RefuseVote] >>: Loop[V]
 type AcceptAppendEntriesBehaviour = (Out[OChan[AckAppendEntries], AcceptAppendEntries] >>: Out[timerReset.type, TimerReset]) >>: Loop[RecFollower]
 type RefuseAppendEntriesBehaviour[V[A] <: RecVar[A]] = Out[OChan[AckAppendEntries], RefuseAppendEntries] >>: Loop[V]

 type Follower = Rec[RecFollower,
   Branch[RpcMessage | TimerExpired, (inbox.type, timeoutChan.type), (
    RequestVote => GrantVoteBehaviour | RefuseVoteBehaviour[RecFollower],
    AppendEntries => AcceptAppendEntriesBehaviour | RefuseAppendEntriesBehaviour[RecFollower],
    TimerExpired => Candidate
   )]
 ]

 type CountVotes = Rec[RecX,
    Branch1[VoteResponse, IChan[VoteResponse], (
        // FIXME Reply channels - need match over VoteResponse to extract
        GrantVote => Loop[RecX] | (Out[OChan[ElectionResult], ElectionInvalid | ElectionWin] >>: PNil),
        RefuseVote => Loop[RecX]
    )]
//    In[IChan[VoteResponse], VoteResponse, (voteResponse: VoteResponse) =>
//      Loop[RecX]
//          // FIXME Reply channels - need match over VoteResponse to extract
//        | (Out[OChan[Int], Int] >>: Loop[RecX])
//    ]
 ]

 type Candidate = Rec[RecElection,
   Out[timerReset.type, TimerReset]
     >>: Par3[
       // FIXME Constrain which reply channel is sent
       Broadcast[RequestVote],
       CountVotes,
       Rec[RecCandidate,
        Branch[RpcMessage | TimerExpired | ElectionResult, (inbox.type, timeoutChan.type, IChan[ElectionResult]), (
            RequestVote => GrantVoteBehaviour | RefuseVoteBehaviour[RecCandidate],
            AppendEntries => AcceptAppendEntriesBehaviour | RefuseAppendEntriesBehaviour[RecCandidate],
            TimerExpired => Loop[RecElection],
            ElectionResult => Leader | (Out[timerReset.type, TimerReset] >>: Loop[RecFollower]) | Loop[RecElection]
        )]
       ]
     ]
 ]

 type Leader =
   Out[timerReset.type, TimerReset]
     >>: Par[
       Broadcast[AppendEntries],
       Rec[RecLeader,
        Branch[RpcMessage | TimerExpired, (inbox.type, timeoutChan.type), (
          AppendEntries => AcceptAppendEntriesBehaviour | RefuseAppendEntriesBehaviour[RecLeader],
          RequestVote => GrantVoteBehaviour | RefuseVoteBehaviour[RecLeader],
          TimerExpired => (Out[timerReset.type, TimerReset] >>: (Loop[RecLeader] | Par[Broadcast[AppendEntries], Loop[RecLeader]]))
        )]
    ]
     ]

 type ServerBehaviour = Par3[
   Out[timerReset.type, TimerReset],
   Timer,
   Follower
 ]

 def timer: Timer = {
   rec(RecX) {
     receive(timerReset) { duration =>
       timerId += 1
       val sleepTime = duration.min + scala.util.Random.nextInt(duration.max - duration.min)

       def waitProcess(expectedId: Int): TimerWait = {
         Thread.sleep(sleepTime)
         if (timerId == expectedId) {
           send(timeoutChan, TimerExpired())
         } else {
           nil
         }
       }

       par(waitProcess(timerId), loop(RecX))
     }
   }
 }

 def broadcast[A <: RpcMessage](msg: A): Broadcast[A] = {
   par(send(peer1, msg), send(peer2, msg))
 }

 def follower: Follower = {
   Logger.log(me, t, "FOLLOWER", "Starting as follower")
   rec(RecFollower) {
     branch((inbox, timeoutChan), (
        (rv: RequestVote) => {
         def deny: RefuseVoteBehaviour[RecFollower] = {
           Logger.log(me, t, "FOLLOWER", s"Denying vote to Server ${rv.from}")
           send(rv.reply, RefuseVote(t))
             >> loop(RecFollower)
         }

         def grant: GrantVoteBehaviour = {
           t = Math.max(t, rv.t1)
           vote = Some(rv.from)
           Logger.log(me, t, "FOLLOWER", s"Granting vote to Server ${rv.from}")

           send(rv.reply, GrantVote(rv.t1))
             >> send(timerReset, electionTimeout)
             >> loop(RecFollower)
         }

         if (rv.t1 < t) deny
         else if (rv.t1 == t) {
           if ((vote.isEmpty || vote.contains(rv.from)) && rv.upToDate) grant
           else deny
         }
         else grant
        },

        (ae: AppendEntries) => {
         Logger.log(me, t, "FOLLOWER", s"Received AppendEntries from Server ${ae.from}")

         def failed: RefuseAppendEntriesBehaviour[RecFollower] = {
           println("AppendEntries failed")
           send(ae.reply, RefuseAppendEntries(t))
             >> loop(RecFollower)
         }

         def success: AcceptAppendEntriesBehaviour = {
           t = Math.max(t, ae.t1)
           send(ae.reply, AcceptAppendEntries(ae.t1))
             >> send(timerReset, electionTimeout)
             >> loop(RecFollower)
         }

         if (ae.t1 < t) failed
         else if (ae.t1 == t) success
         else { vote = None; success }
       },

        (x: TimerExpired) => {
         Logger.log(me, t, "FOLLOWER", "Election timeout expired - transitioning to CANDIDATE")
         candidate
       }
     ))
   }
 }

 def countVotes(voteResponseChan: IChan[VoteResponse], electionResultChan: OChan[ElectionResult]): CountVotes = {
   var votesRemaining = majority - 1

   rec(RecX) {
        branch1(voteResponseChan, (
            (gv: GrantVote) => {
                if (gv.t1 == t) {
                  votesRemaining -= 1
                } else if (gv.t1 > t) {
                    send(electionResultChan, ElectionInvalid()) >> nil
                }
                
                if (votesRemaining == 0) {
                    send(electionResultChan, ElectionWin()) >> nil
                }

                loop(RecX)
            },
            (rv: RefuseVote) => loop(RecX) // FIXME Check for election loss
        ))
   }
 }

 def candidate: Candidate = {
   rec(RecElection) {
     t += 1
     val voteResponseChan = Chan.async[VoteResponse]()
     val electionResultChan = Chan.async[ElectionResult]()

     send(timerReset, candidateTimeout)
       >> par(
         broadcast(RequestVote(voteResponseChan, me, t, true)),
         countVotes(voteResponseChan, electionResultChan),
         rec(RecCandidate) {
            branch((inbox, timeoutChan, electionResultChan), (
                (rv: RequestVote) => {
                    if (rv.t1 > t) {
                     Logger.log(me, t, "CANDIDATE", s"Received RequestVote from Server ${rv.from} (term=${rv.t1} > $t) - reverting to FOLLOWER")

                     t = rv.t1
                     vote = Some(rv.from)

                     send(rv.reply, GrantVote(rv.t1))
                     >> send(timerReset, electionTimeout)
                     >> loop(RecFollower)
                    } else {
                      send(rv.reply, RefuseVote(t))
                      >> loop(RecCandidate)
                    }
                },
                (ae: AppendEntries) => {
                    if (ae.t1 >= t) {
                      t = ae.t1
                      vote = None
        
                      send(ae.reply, AcceptAppendEntries(ae.t1))
                      >> send(timerReset, electionTimeout)
                      >> loop (RecFollower)
                    } else {
                      send(ae.reply, RefuseAppendEntries(t))
                      >> loop (RecCandidate)
                    }
                },
                (_: TimerExpired) => {
                    Logger.log(me, t, "CANDIDATE", s"Election timeout - starting new election")
                    loop(RecElection)
                },
                (er: ElectionResult) => {
                    er match {
                        case ElectionWin() => {
                            Logger.log(me, t, "CANDIDATE", s"Election win! Becoming LEADER")
                            leader
                        }
                        case ElectionInvalid() => {
                            Logger.log(me, t, "CANDIDATE", s"Election invalidated, reverting to FOLLOWER")

                            // FIXME Set term
                            send(timerReset, electionTimeout)
                                >> loop(RecFollower)
                        }
                        case ElectionLoss() => {
                            Logger.log(me, t, "CANDIDATE", s"Election lost - starting new election")
                            loop(RecElection)
                        }
                    }
                }
            ))
         }
       )
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
            (ae: AppendEntries) => {
                if (ae.t1 >= t) {
                Logger.log(me, t, "LEADER", s"Received AppendEntries from Server ${ae.from} (term=${ae.t1}) - reverting to FOLLOWER")
    
                t = ae.t1
                vote = None
    
                send(ae.reply, AcceptAppendEntries(ae.t1))
                >> send(timerReset, electionTimeout)
                >> loop(RecFollower)
                } else {
                send(ae.reply, RefuseAppendEntries(t))
                >> loop(RecLeader)
                }
            },
            (rv: RequestVote) => {
                if (rv.t1 > t) {
                Logger.log(me, t, "LEADER", s"Received RequestVote from Server ${rv.from} (term=${rv.t1} > $t) - reverting to FOLLOWER")
    
                t = rv.t1
                vote = Some(rv.from)
    
                send(rv.reply, GrantVote(t))
                >> send(timerReset, electionTimeout)
                >> loop(RecFollower)
                } else {
                send(rv.reply, RefuseVote(t))
                >> loop(RecLeader)
                }
            },
            (_: TimerExpired) => {
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

// Main object to run the Raft implementation with 3 servers
object Main {
 def main(): Unit = main(Array())

//  @verify(property = "deadlock_free()")
 def system() = {
   // Create inbox channels for each server
   val inbox0 = Chan.async[RpcMessage]()
   val inbox1 = Chan.async[RpcMessage]()
   val inbox2 = Chan.async[RpcMessage]()

   // Create the three servers
   val server0 = new Server(0, inbox0, inbox1, inbox2)
   val server1 = new Server(1, inbox1, inbox0, inbox2)
   val server2 = new Server(2, inbox2, inbox0, inbox1)

   // Spawn all three servers
   par(server0.run(), server1.run(), server2.run())
 }

 def main(args: Array[String]): Unit = {
   eval(system())
 }
}
