
package effpi.examples.branchexample

import effpi.channel.{Channel => Chan, InChannel => IChan, OutChannel => OChan}
import effpi.process._
import effpi.process.dsl._
import scala.concurrent.duration.Duration

package object types {
  type Splitter[C <: IChan[Int | String],
                 OInt <: OChan[Int],
                 OStr <: OChan[String]] =
                  Rec[RecX,
    Branch1[Int | String, C, (
      (v: Int) => Out[OInt, v.type] >>: Loop[RecX],
      (v: String) => Out[OStr, v.type] >>: Loop[RecX]
    )]]

  type Crossroads[C1 <: IChan[Int | String],
                  C2 <: IChan[Int | String],
                  OInt <: OChan[Int],
                  OStr <: OChan[String]] =
    Rec[RecX,
      Branch[Int | String, (C1, C2), (
        (v: Int) => Out[OInt, v.type] >>: Loop[RecX],
        (v: String) => Out[OStr, v.type] >>: Loop[RecX]
      )]
    ]
  
  case class MsgA(v: Int)
  case class MsgB(v: Int)
  case class MsgC(v: String)

  type MyLabelledMessages = MsgA | MsgB | MsgC

  type CaseClasses[C <: IChan[MyLabelledMessages]] =
    Rec[RecX,
      Branch1[MyLabelledMessages, C, (
        (msg: MsgA) => Loop[RecX],
        (msg: MsgB) => Loop[RecX],
        (msg: MsgC) => Loop[RecX]
      )]
    ]
}

package object implementation {
  import types._
  import effpi.process.given  // Required for TypeTest instances used by Branch runtime type matching
  implicit val timeout: Duration = Duration(30, "seconds")

  def splitter(in: IChan[Int | String], outInt: OChan[Int], outStr: OChan[String]): Splitter[in.type, outInt.type, outStr.type] = {
    rec(RecX) {
      branch1(in, (
        (v: Int) => send(outInt, v) >> loop(RecX),
        (v: String) => send(outStr, v) >> loop(RecX)
      ))
    }
  }

  def crossroads(in1: IChan[Int | String], in2: IChan[Int | String], outInt: OChan[Int], outStr: OChan[String]): Crossroads[in1.type, in2.type, outInt.type, outStr.type] = {
    rec(RecX) {
      branch((in1, in2), (
        (v: Int) => send(outInt, v) >> loop(RecX),
        (v: String) => send(outStr, v) >> loop(RecX)
      ))
    }
  }

  def caseClasses(in: IChan[MyLabelledMessages]): CaseClasses[in.type] = {
    rec(RecX) {
      branch1(in, (
        (msg: MsgA) => {
          println(s"  Received MsgA with v = ${msg.v}")
          loop(RecX)
        },
        (msg: MsgB) => {
          println(s"  Received MsgB with v = ${msg.v}")
          loop(RecX)
        },
        (msg: MsgC) => {
          println(s"  Received MsgC with v = ${msg.v}")
          loop(RecX)
        }
      ))
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

    val c = Chan[MyLabelledMessages]()

    eval(
      par(
        caseClasses(c),
        send(c, MsgA(1)) >> send(c, MsgA(2)) >> send(c, MsgC("hello")) >> nil,
        send(c, MsgB(3)) >> send(c, MsgC("world")) >> nil
      )
    )

    println("\n=== All tests completed ===")
  }
}
