
package effpi.examples.branchexample

import effpi.channel.{Channel => Chan, InChannel => IChan, OutChannel => OChan}
import effpi.process._
import effpi.process.dsl._
import scala.concurrent.duration.Duration

package object types {
  type Splitter[C <: IChan[Int | String],
                 OInt <: OChan[Int],
                 OStr <: OChan[String]] =
    Branch1[Int | String, C, (
      (v: Int) => Out[OInt, v.type] >>: PNil,
      (v: String) => Out[OStr, v.type] >>: PNil
    )]

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
}

package object implementation {
  import types._
  implicit val timeout: Duration = Duration(30, "seconds")

  def splitter(in: IChan[Int | String], outInt: OChan[Int], outStr: OChan[String]): Splitter[in.type, outInt.type, outStr.type] = {
    branch1(in, (
      (v: Int) => send(outInt, v) >> nil,
      (v: String) => send(outStr, v) >> nil
    ))
  }

  def crossroads(in1: IChan[Int | String], in2: IChan[Int | String], outInt: OChan[Int], outStr: OChan[String]): Crossroads[in1.type, in2.type, outInt.type, outStr.type] = {
    rec(RecX) {
      branch((in1, in2), (
        (v: Int) => send(outInt, v) >> loop(RecX),
        (v: String) => send(outStr, v) >> loop(RecX)
      ))
    }
  }
}

// To run this example, try:
// sbt "examples/runMain effpi.examples.branchexample.Main"
object Main {
  import implementation._
  def main(): Unit = main(Array())

  def main(args: Array[String]) = {
    val (c1pick, c1drop) = (Chan[Unit](), Chan[Unit]())
    val (c2pick, c2drop) = (Chan[Unit](), Chan[Unit]())
    val (c3pick, c3drop) = (Chan[Unit](), Chan[Unit]())

    implicit val ps = effpi.system.ProcessSystemRunnerImproved()

    //...

    Thread.sleep(30000); ps.kill()
    println("*** ProcessSystem killed after 30 seconds.")
  }
}
