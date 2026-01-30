// package effpi.examples.intersectiontest

// import scala.deriving.Mirror
// import scala.compiletime.ops.int.*
// import effpi.channel.{Channel => Chan, InChannel => IChan, OutChannel => OChan}
// import effpi.process._
// import effpi.process.dsl._
// import effpi.process.consWrap
// import effpi.process.given
// import effpi.verifier.verify
// import scala.concurrent.duration.Duration

// // Define message type as a sealed trait
// // sealed trait Message
// // case class MsgA() extends Message
// // case class MsgB() extends Message
// // case class MsgC() extends Message

// sealed trait Labels1
// sealed trait Labels2
// case class Msg2A() extends Labels1
// case class Msg2B() extends Labels1 with Labels2
// case class Msg2C() extends Labels1 with Labels2

// object IntersectionTest {

//   // type B1 = Branch1[Labels1, IChan[Labels1], (
//   //   (m: Msg2A) => PNil,
//   //   (m: Msg2C) => PNil
//   // )]

//   type B2[C1 <: IChan[Labels1], C2 <: IChan[Labels2]] =
//     Branch[Labels1, (IChan[Labels2], IChan[Labels1]), (
//       (m: Msg2B) => PNil,
//       (m: Msg2C) => PNil
//     )]

//   def impl(in1: IChan[Labels1], in2: IChan[Labels2]): B2[in1.type, in2.type] = {
//     rec(RecX) {
//       branch((in1, in2), (
//         (m: Msg2A) => nil,
//         (m: Msg2B) => nil,
//         (m: Msg2C) => nil
//       ))
//     }
//   }

//   // // ==========================================================================
//   // // Match types for checking overlap
//   // // ==========================================================================

//   // // Check if A is a subtype of B
//   // type IsSubtype[A, B] <: Boolean = A match
//   //   case B => true
//   //   case _ => false

//   // // Count how many types in tuple T accept component C (i.e., C <:< T)
//   // type CountAccepting[C, T <: Tuple] <: Int = T match
//   //   case EmptyTuple => 0
//   //   case head *: tail => IsSubtype[C, head] match
//   //     case true => CountAccepting[C, tail] + 1
//   //     case false => CountAccepting[C, tail]

//   // // Check all components are accepted by exactly one branch
//   // type AllExactlyOne[Components <: Tuple, Branches <: Tuple] <: Boolean = Components match
//   //   case EmptyTuple => true
//   //   case c *: ctail => CountAccepting[c, Branches] match
//   //     case 1 => AllExactlyOne[ctail, Branches]
//   //     case _ => false

//   // // ==========================================================================
//   // // ValidBranch using Mirror.SumOf to extract components
//   // // ==========================================================================

//   // // Simplified ValidBranch that requires A to be a sealed type
//   // trait ValidBranchNew[A, Branches <: Tuple]
//   // object ValidBranchNew {
//   //   given [A, Branches <: Tuple](using
//   //     // Require A to be a sealed type - compiler provides Mirror.SumOf only for sealed types
//   //     mirror: Mirror.SumOf[A],
//   //     // Check that each component of A belongs to exactly one branch
//   //     ev: AllExactlyOne[mirror.MirroredElemTypes, Branches] =:= true
//   //   ): ValidBranchNew[A, Branches] with {}
//   // }

//   // // ==========================================================================
//   // // TESTS
//   // // ==========================================================================

//   // // First, verify we can get the mirror and its components
//   // val mirror = summon[Mirror.SumOf[Labels1]]
//   // print(mirror.MirroredElemTypes)
//   // mirror.MirroredElemTypes should be (MsgA, MsgB, MsgC)

//   // // Test: Valid branches - each message type handled separately
//   // val valid1: ValidBranchNew[Message, MsgA *: MsgB *: MsgC *: EmptyTuple] = implicitly

//   // // Test: Valid branches - grouped but no overlap
//   // val valid2: ValidBranchNew[Message, (MsgA | MsgB) *: MsgC *: EmptyTuple] = implicitly

//   // val valid3: ValidBranchNew[Labels1, Msg2A *: Msg2C *: EmptyTuple] = implicitly
//   // val valid4: ValidBranchNew[Labels2, Msg2B *: Msg2C *: EmptyTuple] = implicitly

//   // // Test: Invalid branches - MsgB appears in both (uncomment to verify failure)
//   // // val invalid1: ValidBranchNew[Message, (MsgA | MsgB) *: (MsgB | MsgC) *: EmptyTuple] = implicitly

//   // // Test: Using a non-sealed type should fail (uncomment to verify)
//   // // type NotSealed = Int | String
//   // // val invalid2: ValidBranchNew[NotSealed, Int *: String *: EmptyTuple] = implicitly

//   // // This fails - Labels1 & Labels2 is an intersection, not a sealed trait
//   // // val invalid3: ValidBranchNew[Labels1 with Labels2, Msg2A *: Msg2B *: Msg2C *: EmptyTuple] = implicitly

//   // // ==========================================================================
//   // // Multi-channel support
//   // // ==========================================================================

//   // // Simulate channel types (simplified - in real code these come from effpi.channel)
//   // trait InChannel[+A]

//   // // Check if component C can be received by at least one channel in Chans
//   // // A channel InChannel[T] can receive C if C <:< T
//   // type CanReceive[C, Chans <: Tuple] <: Boolean = Chans match
//   //   case EmptyTuple => false
//   //   case InChannel[t] *: tail => IsSubtype[C, t] match
//   //     case true => true
//   //     case false => CanReceive[C, tail]
//   //   case _ *: tail => CanReceive[C, tail]  // Skip non-channel types

//   // // Check that ALL components of A can be received by at least one channel
//   // type AllComponentsCovered[Components <: Tuple, Chans <: Tuple] <: Boolean = Components match
//   //   case EmptyTuple => true
//   //   case c *: ctail => CanReceive[c, Chans] match
//   //     case true => AllComponentsCovered[ctail, Chans]
//   //     case false => false

//   // // Full validation for multi-channel branch
//   // trait ValidBranchMulti[A, Chans <: Tuple, Branches <: Tuple]
//   // object ValidBranchMulti {
//   //   given [A, Chans <: Tuple, Branches <: Tuple](using
//   //     // A must be a sealed type
//   //     mirror: Mirror.SumOf[A],
//   //     // All components of A must be receivable by at least one channel
//   //     chansCover: AllComponentsCovered[mirror.MirroredElemTypes, Chans] =:= true,
//   //     // Each component must belong to exactly one branch (no overlap)
//   //     noOverlap: AllExactlyOne[mirror.MirroredElemTypes, Branches] =:= true
//   //   ): ValidBranchMulti[A, Chans, Branches] with {}
//   // }

//   // // ==========================================================================
//   // // Multi-channel tests
//   // // ==========================================================================

//   // // Scenario: Branch over Message with two channels
//   // // ch1: InChannel[MsgA | MsgB] - can receive MsgA or MsgB
//   // // ch2: InChannel[MsgC]        - can receive MsgC
//   // // Together they cover all of Message

//   // type Ch1 = InChannel[MsgA | MsgB]
//   // type Ch2 = InChannel[MsgC]
//   // type Ch3 = InChannel[MsgB | MsgC]

//   // // Valid: channels cover all components, branches partition without overlap
//   // val multi1: ValidBranchMulti[Message, Ch1 *: Ch2 *: EmptyTuple, MsgA *: MsgB *: MsgC *: EmptyTuple] = implicitly

//   // // Valid: grouped branches, channels still cover
//   // val multi2: ValidBranchMulti[Message, Ch1 *: Ch2 *: EmptyTuple, (MsgA | MsgB) *: MsgC *: EmptyTuple] = implicitly

//   // // Valid: overlapping channel coverage is OK (MsgB receivable on both Ch1 and Ch3)
//   // val multi3: ValidBranchMulti[Message, Ch1 *: Ch3 *: EmptyTuple, MsgA *: MsgB *: MsgC *: EmptyTuple] = implicitly

//   // // Invalid: channels don't cover MsgC (uncomment to verify failure)
//   // // val multiInvalid1: ValidBranchMulti[Message, Ch1 *: EmptyTuple, MsgA *: MsgB *: MsgC *: EmptyTuple] = implicitly

//   // // Invalid: branches overlap at MsgB (uncomment to verify failure)
//   // // val multiInvalid2: ValidBranchMulti[Message, Ch1 *: Ch2 *: EmptyTuple, (MsgA | MsgB) *: (MsgB | MsgC) *: EmptyTuple] = implicitly
// }
