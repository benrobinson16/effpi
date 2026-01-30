
package effpi.examples.timeouttests

import effpi.channel.{Channel => Chan, InChannel => IChan, OutChannel => OChan}
import effpi.process._
import effpi.process.given
import effpi.process.dsl._
import scala.concurrent.duration.Duration

package object types {

  // Message types. Must be sealed!
  type Message = MsgA | MsgB
  sealed case class MsgA(v: Int)
  sealed case class MsgB(v: String)

    type Receiver[C <: IChan[Message], C2 <: OChan[String]] =
        Rec[RecX,
            WithTimeout[
                Branch[Message, Tuple1[C], (
                    (m: MsgA) => Loop[RecX],
                    (m: MsgB) => Loop[RecX]
                )],
                Out[C2, String]
            ]
        ]
}

package object implementation {
  import types._
  implicit val timeout: Duration = Duration(20, "seconds")

    def receiver(in: IChan[Message], out: OChan[String]): Receiver[in.type, out.type] = {
      rec(RecX) {
        implicit val timeout = Duration(1, "seconds")
        withTimeout(
            branch1(in, (
                (m: MsgA) => {
                    println(s"Received MsgA with value ${m.v}")
                    loop(RecX)
                },
                (m: MsgB) => {
                    println(s"Received MsgB with value ${m.v}")
                    loop(RecX)
                }
            )),
            {
                println("No message received within timeout, sending 'Done'")
                send(out, "Done")
            }
        )
            // branch(Tuple1(in), (
            //   (m: MsgA) => {
            //     println(s"Received MsgA with value ${m.v}")
            //     loop(RecX)
            //   },
            //   (m: MsgB) => {
            //     println(s"Received MsgB with value ${m.v}")
            //     loop(RecX)
            //   }
            // ), () => {
            //     println("No message received within timeout, sending 'Done'")
            //     send(out, "Done")
            // })
        }
    }

    def receiver1(in: IChan[Message], out: OChan[String]) = {
      rec(RecX) {
        implicit val timeout = Duration(1, "seconds")
        withTimeout(
        receive(in) {
          case m: MsgA => {
            println(s"Received MsgA with value ${m.v}")
            loop(RecX)
          }
          case m: MsgB => {
            println(s"Received MsgB with value ${m.v}")
            loop(RecX)
          }
        }, {
            println("No message received within timeout, sending 'Done'")
                send(out, "Done")
        })
      }
    }

    def producer(out: OChan[Message]): Process = {
      send(out, MsgA(42)) >>
      send(out, MsgB("Hello")) >>
      nil
    }

    def consumer(in: IChan[String]): Process = {
      rec(RecY) {
        receive(in) { x =>
          println(s"Consumer received String: $x")
          loop(RecY)
        }
      }
    }
}

// To run this example, try:
// sbt "examples/runMain effpi.examples.timeouttests.Main"
object Main {
  import types._
  import implementation._
  def main(): Unit = main(Array())

  def main(args: Array[String]) = {
    println("=== Basic Branch Tests ===")

    val c1 = Chan[Message]()
    val c2 = Chan[Int]()
    val c3 = Chan[String]()

    try {

    eval(
      par(
        producer(c1),
        receiver1(c1, c3),
        consumer(c3)
      )
    )
    } catch {
        case _ =>
            println(s"Main process interrupted")
        
    }

    println("\n=== All tests completed ===")
  }
}
