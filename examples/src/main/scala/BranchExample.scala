
package effpi.examples.branchexample

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

  type Splitter[C <: IChan[Message],
                OInt <: OChan[Int],
                OStr <: OChan[String]] =
    Rec[RecX,
      Branch1[Message, C, (
        (m: MsgA) => Out[OInt, m.v.type] >>: Loop[RecX],
        (m: MsgB) => Out[OStr, m.v.type] >>: Loop[RecX]
      )]
    ]

  // type T123 = Out[OChan[Int], String]

  type Crossroads[C1 <: IChan[Message],
                  C2 <: IChan[Message],
                  OInt <: OChan[Int],
                  OStr <: OChan[String]] =
    Rec[RecX,
      Branch[Message, (C1, C2), (
        (m: MsgB) => Out[OStr, m.v.type] >>: Loop[RecX],
        (m: MsgB) => Out[OStr, m.v.type] >>: Loop[RecX]
      )]
    ]

  type Producer[A, C <: OChan[A]] =
    Rec[RecX, Out[C, A] >>: Loop[RecX]]

  type Consumer[A, C <: IChan[A]] =
    Rec[RecY, In[C, A, (x: A) => Loop[RecY]]]
}

package object implementation {
  import types._
  implicit val timeout: Duration = Duration(30, "seconds")

  def splitter(in: IChan[Message], outInt: OChan[Int], outStr: OChan[String]): Splitter[in.type, outInt.type, outStr.type] = {
    rec(RecX) {
      branch1(in, (
        (m: MsgA) => send(outInt, m.v) >> loop(RecX),
        (m: MsgB) => send(outStr, m.v) >> loop(RecX)
      ))
    }
  }

  // def crossroads(in1: IChan[Message], in2: IChan[Message], outInt: OChan[Int], outStr: OChan[String]): Crossroads[in1.type, in2.type, outInt.type, outStr.type] = {
  //   rec(RecX) {
  //     branch((in1, in2), (
  //       (msg: MsgA) => send(outInt, msg.v) >> loop(RecX),
  //       (msg: MsgB) => send(outStr, msg.v) >> loop(RecX)
  //     ))
  //   }
  // }

  def producer(out: OChan[Message]): Producer[Message, out.type] = {
    rec(RecX) {
      if (scala.util.Random.nextBoolean()) {
        send(out, MsgA(42)) >> loop(RecX)
      } else {
        send(out, MsgB("hello")) >> loop(RecX)
      }
    }
  }

  def consumerInt(in: IChan[Int]): Consumer[Int, in.type] = {
    rec(RecY) {
      receive(in) { x =>
        println(s"Consumer received Int: $x")
        loop(RecY)
      }
    }
  }

  def consumerStr(in: IChan[String]): Consumer[String, in.type] = {
    rec(RecY) {
      receive(in) { x =>
        println(s"Consumer received String: $x")
        loop(RecY)
      }
    }
  }
}

// To run this example, try:
// sbt "examples/runMain effpi.examples.branchexample.Main"
object Main {
  import types._
  import implementation._
  def main(): Unit = main(Array())

  def main(args: Array[String]) = {
    println("=== Basic Branch Tests ===")

    val c1 = Chan[Message]()
    val c2 = Chan[Int]()
    val c3 = Chan[String]()

    eval(
      par(
        producer(c1),
        splitter(c1, c2, c3),
        consumerInt(c2),
        consumerStr(c3)
      )
    )

    println("\n=== All tests completed ===")
  }
}
