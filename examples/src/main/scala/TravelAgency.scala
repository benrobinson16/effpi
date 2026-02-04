package effpi.examples.travelagency

import effpi.channel.{Channel => Chan, InChannel => IChan, OutChannel => OChan}
import effpi.process._
import effpi.process.given
import effpi.process.dsl._

package types {
  case class Accept()
  case class Reject()
  type Decision = Accept | Reject

  type TravelAgency[C1 <: IChan[Decision], C2 <: OChan[String]] =
    Branch1[Decision, C1, (
      Accept => Out[C2, String],
      Reject => PNil
    )]

  type Client[C1 <: OChan[Decision], C2 <: IChan[String]] =
    Out[C1, Decision] >>: (In[C2, String, (_s: String) => PNil] | PNil)
}

package implementation {
  import types._
  import scala.concurrent.duration.Duration

  implicit val timeout: Duration = Duration(30, "seconds")

  def travelAgency(c1: IChan[Decision], c2: OChan[String]): TravelAgency[c1.type, c2.type] = {
    branch1(c1, (
      (a: Accept) => {
        println("TravelAgency: Client accepted the offer.")
        send(c2, "Your ticket")
      },
      (r: Reject) => {
        println("TravelAgency: Client rejected the offer.")
        nil
      }
    ))
  }

  def client(c1: OChan[Decision], c2: IChan[String]): Client[c1.type, c2.type] = {
    if (scala.util.Random.nextBoolean()) {
      println("Client: Accepting the offer.")

      send(c1, Accept()) >> receive(c2) { ticket => 
        println(s"Client: Received ticket: ${ticket}")
        nil
      }
    } else {
      println("Client: Rejecting the offer.")

      send(c1, Reject()) >> nil
    }
  }
}

// sbt "examples/runMain effpi.examples.travelagency.Main"
object Main {
  import types._
  import implementation._
  def main(): Unit = main(Array())

  def main(args: Array[String]) = {
    val c1 = Chan[Decision]()
    val c2 = Chan[String]()

    eval(
      par(
        client(c1, c2),
        travelAgency(c1, c2)
      )
    )
  }
}
