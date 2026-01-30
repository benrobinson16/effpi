// package effpi.examples.distincttest

// import effpi.channel.{Channel => Chan, InChannel => IChan, OutChannel => OChan}
// import effpi.process._
// import effpi.process.dsl._
// import effpi.process.given
// import scala.concurrent.duration.Duration

// // Define a hierarchy
// case class MsgA()
// case class MsgB()
// case class MsgC()
// type Message = MsgA | MsgB | MsgC

// object DistinctTypesTest {
//   implicit val timeout: Duration = Duration(30, "seconds")

//   // These processes should compile fine...

//   def validBranch1(in: IChan[Message]) = {
//     branch1(in, (
//       (v: MsgA) => nil,
//       (v: MsgB) => nil,
//       (v: MsgC) => nil
//     ))
//   }

//   def validBranch2(in: IChan[Message]) = {
//     branch1(in, (
//       (v: MsgA | MsgB) => nil,
//       (v: MsgC) => nil
//     ))
//   }

//   def validBranch3(in: IChan[Message]) = {
//     branch1(in, Tuple1(
//       (v: MsgA | MsgB | MsgC) => nil
//     ))
//   }

//   // These processes should not compile...

//   // def invalidBranch1(in: IChan[Message]) = {
//   //   branch1(in, (
//   //     (v: MsgA) => nil,
//   //     (v: MsgB | MsgA) => nil,
//   //     (v: MsgC) => nil
//   //   ))
//   // }

//   // Unfortunately still compiles
//   def invalidBranch2(in: IChan[Message]) = {
//     branch1(in, (
//       (v: MsgA | MsgB) => nil,
//       (v: MsgB | MsgC) => nil
//     ))
//   }
// }
