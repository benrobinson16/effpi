package effpi.examples.loopSystem

import effpi.channel.{Channel => Chan, InChannel => IChan, OutChannel => OChan}
import effpi.process._
import effpi.process.dsl._
import effpi.verifier.verify

package object types {
    case class MsgA(v: Int)
    case class MsgB(v: String)
    type Message = MsgA | MsgB

    type LoopSystem[C <: Chan[MsgA]] =
        Par[
            Rec[RecX, Out[C, MsgA] >>: Loop[RecX]],
            Rec[RecY,
                Branch1[Message, C, (
                    (msg: MsgA) => Loop[RecY],
                    (msg: MsgB) => PNil
                )]
            ]
        ]
    
    @verify(property = "deadlock_free()") // Success
    def loopSystemCHK(c: Chan[MsgA]): LoopSystem[c.type] = ???

    // With channel created as Chan[MsgA].
    // Only has In(..) from chan to the MsgA branch.

    // Par(Rec(RecVar(effpi.process.RecX),Out(TypeVar(c,AppType(TypeRef(TermRef(TermRef(ThisType(TypeRef(NoPrefix,module class <root>)),object effpi),object channel),class Channel),List(TypeRef(TypeRef(TermRef(ThisType(TypeRef(NoPrefix,module class types)),object package),class MsgA))))),GroundType(TypeRef(TermRef(ThisType(TypeRef(NoPrefix,module class types)),object package),class MsgA)),RecVar(effpi.process.RecX))),
    //     Rec(RecVar(effpi.process.RecY),Branch(Map(In(TypeVar(c,AppType(TypeRef(TermRef(TermRef(ThisType(TypeRef(NoPrefix,module class <root>)),object effpi),object channel),class Channel),List(TypeRef(TypeRef(TermRef(ThisType(TypeRef(NoPrefix,module class types)),object package),class MsgA))))),GroundType(TypeRef(TermRef(ThisType(TypeRef(NoPrefix,module class types)),object package),class MsgA))) -> RecVar(effpi.process.RecY)))))

    type LoopSystem2[C <: Chan[Message]] =
        Par[
            Rec[RecX, Out[C, MsgA] >>: Loop[RecX]],
            Rec[RecY,
                Branch1[Message, C, (
                    (msg: MsgA) => Loop[RecY],
                    (msg: MsgB) => PNil
                )]
            ]
        ]
    
    @verify(property = "deadlock_free()") // Expect success, but fails FIXME
    def loopSystem2CHK(c: Chan[Message]): LoopSystem2[c.type] = ???

    // With channel created as Chan[Message].
    // Has In(..) for both MsgA and MsgB because an external process could use the channel to communicate MsgB (i.e. STOP message)

    // Par(Rec(RecVar(effpi.process.RecX),Out(TypeVar(c,AppType(TypeRef(TermRef(TermRef(ThisType(TypeRef(NoPrefix,module class <root>)),object effpi),object channel),class Channel),List(OrType(TypeRef(TypeRef(TermRef(ThisType(TypeRef(NoPrefix,module class types)),object package),class MsgA)),TypeRef(TypeRef(TermRef(ThisType(TypeRef(NoPrefix,module class types)),object package),class MsgB)))))),GroundType(TypeRef(TermRef(ThisType(TypeRef(NoPrefix,module class types)),object package),class MsgA)),RecVar(effpi.process.RecX))),
    //     Rec(RecVar(effpi.process.RecY),Branch(Map(In(TypeVar(c,AppType(TypeRef(TermRef(TermRef(ThisType(TypeRef(NoPrefix,module class <root>)),object effpi),object channel),class Channel),List(OrType(TypeRef(TypeRef(TermRef(ThisType(TypeRef(NoPrefix,module class types)),object package),class MsgA)),TypeRef(TypeRef(TermRef(ThisType(TypeRef(NoPrefix,module class types)),object package),class MsgB)))))),GroundType(TypeRef(TermRef(ThisType(TypeRef(NoPrefix,module class types)),object package),class MsgA))) -> RecVar(effpi.process.RecY),
    //                                               In(TypeVar(c,AppType(TypeRef(TermRef(TermRef(ThisType(TypeRef(NoPrefix,module class <root>)),object effpi),object channel),class Channel),List(OrType(TypeRef(TypeRef(TermRef(ThisType(TypeRef(NoPrefix,module class types)),object package),class MsgA)),TypeRef(TypeRef(TermRef(ThisType(TypeRef(NoPrefix,module class types)),object package),class MsgB)))))),GroundType(TypeRef(TermRef(ThisType(TypeRef(NoPrefix,module class types)),object package),class MsgB))) -> End))))

    // The issue seems to be that we're not inspecting enough to track what the
    // types are of the messages that actually get sent on each channel when
    // verifying deadlock freedom?
}