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


type RPC = RequestVote | AppendEntries
case class RequestVote(reply: OChan[VoteResponse], from: Int, t: Int, upToDate: Boolean)
case class AppendEntries(reply: OChan[AckAppendEntries], from: Int, t: Int)

type VoteResponse = GrantVote | RefuseVote
case class GrantVote(t: Int)
case class RefuseVote(t: Int)

type AckAppendEntries = AcceptAppendEntries | RefuseAppendEntries
case class AcceptAppendEntries(t: Int)
case class RefuseAppendEntries(t: Int)

// FIXME Do we need these?
type ElectionResult = ElectionWin | ElectionLoss | ElectionInvalid
case class ElectionWin()
case class ElectionLoss()
case class ElectionInvalid()

case class TimerExpired()
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

class Server[Inbox <: Chan[RPC], Peer1 <: OChan[RPC], Peer2 <: OChan[RPC]](me: Int, val inbox: Inbox, val peer1: Peer1, val peer2: Peer2) {
 val timerReset = Chan.async[TimerReset]()
 val timeoutChan = Chan[TimerExpired]()
 val majority = 2
 var timerId = 0
 var t = 0
 var vote: Option[Int] = None

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

//  type Timer = Rec[RecX,
//     CatchTimeout[
//       In[timerReset.type, TimerReset, TimerReset => Loop[RecX]],
//       Out[timeoutChan.type, TimerExpired] >>: Loop[RecX]
//     ]
//   ]

 // FIXME Reply channels - need to pass these in, after matching against RPC and extracting
 type GrantVoteBehaviour = (Out[OChan[VoteResponse], GrantVote] >>: Out[timerReset.type, TimerReset]) >>: Loop[RecFollower]
 type RefuseVoteBehaviour[V[A] <: RecVar[A]] = Out[OChan[VoteResponse], RefuseVote] >>: Loop[V]
 type AcceptAppendEntriesBehaviour = (Out[OChan[AckAppendEntries], AcceptAppendEntries] >>: Out[timerReset.type, TimerReset]) >>: Loop[RecFollower]
 type RefuseAppendEntriesBehaviour[V[A] <: RecVar[A]] = Out[OChan[AckAppendEntries], RefuseAppendEntries] >>: Loop[V]

 type Follower = Rec[RecFollower,
   Branch[RPC | TimerExpired, (inbox.type, timeoutChan.type), (
    RequestVote => GrantVoteBehaviour | RefuseVoteBehaviour[RecFollower],
    AppendEntries => AcceptAppendEntriesBehaviour | RefuseAppendEntriesBehaviour[RecFollower],
    TimerExpired => Candidate
   )]
 ]

 type Candidate = Rec[RecElection,
   Out[timerReset.type, TimerReset]
     >>: Par[
       // FIXME Constrain which reply channel is sent
       Broadcast[RequestVote],
       Rec[RecCandidate,
        Branch[RPC | TimerExpired | VoteResponse, (inbox.type, timeoutChan.type, IChan[VoteResponse]), (
            RequestVote => GrantVoteBehaviour | RefuseVoteBehaviour[RecCandidate],
            AppendEntries => AcceptAppendEntriesBehaviour | RefuseAppendEntriesBehaviour[RecCandidate],
            TimerExpired => Loop[RecElection],
            GrantVote => Leader | (Out[timerReset.type, TimerReset] >>: Loop[RecFollower]) | Loop[RecCandidate],
            RefuseVote => Loop[RecCandidate]
        )]
       ]
     ]
 ]

 type Leader =
   Out[timerReset.type, TimerReset]
     >>: Par[
       Broadcast[AppendEntries],
       Rec[RecLeader,
        Branch[RPC | TimerExpired, (inbox.type, timeoutChan.type), (
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
      receive(timerReset) { tr =>
        var millis = tr.min + scala.util.Random.nextInt(tr.max - tr.min)

        rec(RecY) {
          catchTimeout(
            {
              implicit val timeout = Duration(millis.toLong, "millis")
              receive(timerReset) { newTr =>
                millis = newTr.min + scala.util.Random.nextInt(newTr.max - newTr.min)
                loop(RecY)
              }
            },
            {
              // Timer expires!
              send(timeoutChan, TimerExpired()) >> loop(RecX)
            }
          )
        }  
      }
    }


  //   var x: Option[Int] = None

  //  rec(RecX) {
  //   catchTimeout(
  //     {
  //       implicit val timeout = if (x.isEmpty) Duration.Inf else Duration(x.get.toLong, "millis")
  //       receive(timerReset) { duration => 
  //         x = Some(duration.min + scala.util.Random.nextInt(duration.max - duration.min))
  //         loop(RecX)
  //       }
  //     },
  //     {
  //       x = None
  //       send(timeoutChan, TimerExpired()) >> loop(RecX)
  //     }
  //   )
  //  }
 }

 def broadcast[A <: RPC](msg: A): Broadcast[A] = {
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

        (ae: AppendEntries) => {
         Logger.log(me, t, "FOLLOWER", s"Received AppendEntries from Server ${ae.from}")

         def failed: RefuseAppendEntriesBehaviour[RecFollower] = {
           println("AppendEntries failed")
           send(ae.reply, RefuseAppendEntries(t))
             >> loop(RecFollower)
         }

         def success: AcceptAppendEntriesBehaviour = {
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

//  def countVotes(voteResponseChan: IChan[VoteResponse], electionResultChan: OChan[ElectionResult]): CountVotes = {
//    var votesRemaining = majority - 1

//    rec(RecX) {
//         branch1(voteResponseChan, (
            
//         ))
//    }
//  }

 def candidate: Candidate = {
   rec(RecElection) {
     t += 1
     val voteResponseChan = Chan.async[VoteResponse]()
     var votesRemaining = majority - 1
    //  val electionResultChan = Chan.async[ElectionResult]()

     send(timerReset, candidateTimeout)
       >> par(
         broadcast(RequestVote(voteResponseChan, me, t, true)),
        //  countVotes(voteResponseChan, electionResultChan),
         rec(RecCandidate) {

            branch((inbox, timeoutChan, voteResponseChan), (
                (rv: RequestVote) => {
                    if (rv.t > t) {
                     Logger.log(me, t, "CANDIDATE", s"Received RequestVote from Server ${rv.from} (term=${rv.t} > $t) - reverting to FOLLOWER")

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
                (ae: AppendEntries) => {
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
                (_: TimerExpired) => {
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
                (rv: RefuseVote) => loop(RecCandidate)
                // (er: ElectionResult) => {
                //     er match {
                //         case ElectionWin() => {
                //             Logger.log(me, t, "CANDIDATE", s"Election win! Becoming LEADER")
                //             leader
                //         }
                //         case ElectionInvalid() => {
                //             Logger.log(me, t, "CANDIDATE", s"Election invalidated, reverting to FOLLOWER")

                //             // FIXME Set term
                //             send(timerReset, electionTimeout)
                //                 >> loop(RecFollower)
                //         }
                //         case ElectionLoss() => {
                //             Logger.log(me, t, "CANDIDATE", s"Election lost - starting new election")
                //             loop(RecElection)
                //         }
                //     }
                // }
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
            (rv: RequestVote) => {
                if (rv.t > t) {
                Logger.log(me, t, "LEADER", s"Received RequestVote from Server ${rv.from} (term=${rv.t} > $t) - reverting to FOLLOWER")
    
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
   eval(system())
 }
}
