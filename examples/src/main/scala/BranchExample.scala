
package effpi.examples.branchexample

import effpi.channel.{Channel => Chan, InChannel => IChan, OutChannel => OChan}
import effpi.process._
import effpi.process.dsl._
import scala.concurrent.duration.Duration

package object types {
    type IntCase[OInt <: OChan[Int]] = Int => Out[OInt, Int] >>: PNil
    type StrCase[OStr <: OChan[String]] = String => Out[OStr, String] >>: PNil

  type Splitter[C <: IChan[Int | String],
                OInt <: OChan[Int],
                OStr <: OChan[String]] =
    Branch[C, Int | String, Match[Int, IntCase[OInt]] <>:
                            Match[String, StrCase[OStr]]]
}

package object implementation {
  import types._
  implicit val timeout: Duration = Duration(30, "seconds")

  def splitter[C <: IChan[Int | String], OInt <: OChan[Int], OStr <: OChan[String]](
    in: C, outInt: OInt, outStr: OStr
  ): Splitter[C, OInt, OStr] = {
    branch(in)(
      branchCase((v: Int) => send(outInt, v) >> nil) <> branchCase((v: String) => send(outStr, v) >> nil)
    )
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
