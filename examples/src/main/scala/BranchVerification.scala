package effpi.examples.branchverification

import effpi.channel.{Channel => Chan, InChannel => IChan, OutChannel => OChan}
import effpi.process._
import effpi.process.dsl._
import effpi.verifier.verify

package object types {
    case class MsgA(v: Int)
    case class MsgB(v: String)
    type Message = MsgA | MsgB

    type Producer[A, C <: OChan[A]] =
        Rec[RecX, Out[C, A] >>: Loop[RecX]]

    type Consumer[A, C <: IChan[A]] =
        Rec[RecY, In[C, A, (x: A) => Loop[RecY]]]

    // Splitter...
        
    type Splitter[InC <: IChan[Message],
                  OutA <: OChan[MsgA],
                  OutB <: OChan[MsgB]] =
        Rec[RecZ,
            Branch1[Message, InC, (
                (m: MsgA) => Out[OutA, m.type] >>: Loop[RecZ],
                (m: MsgB) => Out[OutB, m.type] >>: Loop[RecZ]
            )]
        ]

    @verify(property = "forwarding(in)(outA, outB)")
    def splitterCHK(in: IChan[Message],
                    outA: OChan[MsgA],
                    outB: OChan[MsgB]): Splitter[in.type, outA.type, outB.type] = ???

    @verify(property = "deadlock_free()")
    def splitterSystemCHK(in: Chan[Message],
                          outA: Chan[MsgA],
                          outB: Chan[MsgB]): Par4[
      Producer[Message, in.type],
      Splitter[in.type, outA.type, outB.type],
      Consumer[MsgA, outA.type],
      Consumer[MsgB, outB.type]
    ] = ???

    // Merger...

    type Merger[InA <: IChan[MsgA], InB <: IChan[MsgB], OutC <: OChan[Message]] =
        Rec[RecX,
            Branch[Message, (InA, InB), Tuple1[
                (m: Message) => Out[OutC, m.type] >>: Loop[RecX]
            ]]]

    @verify(property = "forwarding(inA, inB)(outC)")
    def mergerCHK(inA: IChan[MsgA],
                  inB: IChan[MsgB],
                  outC: OChan[Message]): Merger[inA.type, inB.type, outC.type] = ???

    @verify(property = "deadlock_free()")
    def mergerSystemCHK(inA: Chan[MsgA],
                        inB: Chan[MsgB],
                        outC: Chan[Message]): Par4[
      Producer[MsgA, inA.type],
      Producer[MsgB, inB.type],
      Merger[inA.type, inB.type, outC.type],
      Consumer[Message, outC.type]
    ] = ???

    // Combined Splitter and Merger...

    type SplitterMerger[InC <: IChan[Message],
                        MiddleA <: Chan[MsgA],
                        MiddleB <: Chan[MsgB],
                        OutC <: OChan[Message]] =
        Par[
            Splitter[InC, MiddleA, MiddleB],
            Merger[MiddleA, MiddleB, OutC]
        ]

    @verify(property = "forwarding(in)(out)") // Expect failure - could reorder messages by accepting new input on InC before outputting previous message on OutC
    def splitterMergerCHK(in: IChan[Message],
                          middleA: Chan[MsgA],
                          middleB: Chan[MsgB],
                          out: OChan[Message]): SplitterMerger[in.type, middleA.type, middleB.type, out.type] = ???

    // @verify(property = "deadlock_free()")
    // def splitterMergerSystemCHK(in: Chan[Message],
    //                             middleA: Chan[MsgA],
    //                             middleB: Chan[MsgB],
    //                             out: Chan[Message]): Par3[
    //     Producer[Message, in.type],
    //     SplitterMerger[in.type, middleA.type, middleB.type, out.type],
    //     Consumer[Message, out.type]
    // ] = ???
    
    // A Splitter-Merger forwarder that ensures messages are forwarded in order

    type ControlledSplitter[InC <: IChan[Message],
                            OutA <: OChan[MsgA],
                            OutB <: OChan[MsgB],
                            Ctrl <: IChan[Unit]] =
        Rec[RecX,
            Branch1[Message, InC, (
                (m: MsgA) => Out[OutA, m.type] >>: In[Ctrl, Unit, (x: Unit) => Loop[RecX]],
                (m: MsgB) => Out[OutB, m.type] >>: In[Ctrl, Unit, (x: Unit) => Loop[RecX]],
            )]
        ]

    type ControlledMerger[InA <: IChan[MsgA],
                          InB <: IChan[MsgB],
                          OutC <: OChan[Message],
                          Ctrl <: OChan[Unit]] =
        Rec[RecX,
            Branch[Message, (InA, InB), Tuple1[
                (m: Message) => Out[OutC, m.type] >>: Out[Ctrl, Unit] >>: Loop[RecX]
            ]]
        ]

    type OrderedForwarder[InC <: IChan[Message],
                          MiddleA <: Chan[MsgA],
                          MiddleB <: Chan[MsgB],
                          OutC <: OChan[Message],
                          Ctrl <: Chan[Unit]] =
        Par[
            ControlledSplitter[InC, MiddleA, MiddleB, Ctrl],
            ControlledMerger[MiddleA, MiddleB, OutC, Ctrl]
        ]

    @verify(property = "forwarding(in)(out)")
    def orderedForwarderCHK(in: IChan[Message],
                            middleA: Chan[MsgA],
                            middleB: Chan[MsgB],
                            out: OChan[Message],
                            ctrl: Chan[Unit]): OrderedForwarder[in.type, middleA.type, middleB.type, out.type, ctrl.type] = ???

    // Let's test this further by forwarding messages from the splitter to the merger
    // via new channels...

    type SimpleForwarder[A, InC <: IChan[A], OutC <: OChan[A]] =
        Rec[RecX, In[InC, A, (m: A) => Out[OutC, m.type] >>: Loop[RecX]]]

    type SeparatedOrderedForwarder[InC <: IChan[Message],
                                    MiddleA1 <: Chan[MsgA],
                                    MiddleA2 <: Chan[MsgA],
                                    MiddleB1 <: Chan[MsgB],
                                    MiddleB2 <: Chan[MsgB],
                                    OutC <: OChan[Message],
                                    Ctrl <: Chan[Unit]] =
        Par4[
            ControlledSplitter[InC, MiddleA1, MiddleB1, Ctrl],
            Rec[RecX, In[MiddleA1, MsgA, (m: MsgA) => Out[MiddleA2, m.type] >>: Loop[RecX]]],
            Rec[RecX, In[MiddleB1, MsgB, (m: MsgB) => Out[MiddleB2, m.type] >>: Loop[RecX]]],
            ControlledMerger[MiddleA2, MiddleB2, OutC, Ctrl]
        ]

    @verify(property = "forwarding(in)(out)") // Expect success, but fails FIXME
    def separatedOrderedForwarderCHK(in: IChan[Message],
                                     middleA1: Chan[MsgA],
                                     middleA2: Chan[MsgA],
                                     middleB1: Chan[MsgB],
                                     middleB2: Chan[MsgB],
                                     out: OChan[Message],
                                     ctrl: Chan[Unit]): SeparatedOrderedForwarder[in.type, middleA1.type, middleA2.type, middleB1.type, middleB2.type, out.type, ctrl.type] = ???

    // Let's try a forwarder of the same design but without the branch operation

    type NondeterministicForwarder[InC <: IChan[Message], MiddleA1 <: Chan[Message], MiddleA2 <: Chan[Message], MiddleB1 <: Chan[Message], MiddleB2 <: Chan[Message], OutC <: OChan[Message], Ctrl <: Chan[Unit]] =
        Par5[
            Rec[RecX, In[InC, Message, (m: Message) => (Out[MiddleA1, m.type] >>: In[Ctrl, Unit, (x: Unit) => Loop[RecX]])
                                                        | (Out[MiddleB1, m.type] >>: In[Ctrl, Unit, (x: Unit) => Loop[RecX]])]],
            Rec[RecX, In[MiddleA1, Message, (m: Message) => Out[MiddleA2, m.type] >>: Loop[RecX]]],
            Rec[RecX, In[MiddleB1, Message, (m: Message) => Out[MiddleB2, m.type] >>: Loop[RecX]]],
            Rec[RecX, In[MiddleA2, Message, (m: Message) => Out[OutC, m.type] >>: Out[Ctrl, Unit] >>: Loop[RecX]]],
            Rec[RecX, In[MiddleB2, Message, (m: Message) => Out[OutC, m.type] >>: Out[Ctrl, Unit] >>: Loop[RecX]]],
        ]

    @verify(property = "forwarding(in)(out)") // Expect success, but fails FIXME
    def nondeterministicForwarderCHK(in: IChan[Message],
                                    middleA1: Chan[Message],
                                    middleA2: Chan[Message],
                                    middleB1: Chan[Message],
                                    middleB2: Chan[Message],
                                    out: OChan[Message],
                                    ctrl: Chan[Unit]): NondeterministicForwarder[in.type, middleA1.type, middleA2.type, middleB1.type, middleB2.type, out.type, ctrl.type] = ???
}