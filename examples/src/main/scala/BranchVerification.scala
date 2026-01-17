
package effpi.examples.branchverification

import effpi.channel.{Channel => Chan, InChannel => IChan, OutChannel => OChan}
import effpi.process._
import effpi.process.dsl._
import effpi.verifier.verify

package object types {
    case class MsgA(v: Int)
    case class MsgB(v: String)
    type Message = MsgA | MsgB

    // Producers and consumers...

    type Producer[A, C <: OChan[A]] =
        Rec[RecX, Out[C, A] >>: Loop[RecX]]

    type Consumer[A, C <: IChan[A]] =
        Rec[RecX, In[C, A, (x: A) => Loop[RecX]]]

    type ProducerConsumer[C <: Chan[Message]] =
        Par[Producer[Message, C], Consumer[Message, C]]

    private def producerConsumerCHK(c: Chan[Message]): ProducerConsumer[c.type] = ???

    // Splitter

    type Splitter[In <: IChan[Message], OutA <: OChan[MsgA], OutB <: OChan[MsgB]] =
        Rec[RecX,
            Branch1[Message, In, (
                (m: MsgA) => Out[OutA, m.type] >>: Loop[RecX],
                (m: MsgB) => Out[OutB, m.type] >>: Loop[RecX]
            )]
        ]

    @verify(property = "forwarding(in)(outA, outB)")
    def splitterCHK(in: IChan[Message],
                    outA: OChan[MsgA],
                    outB: OChan[MsgB]): Splitter[in.type,
                                                outA.type,
                                                outB.type] = ???

    type SplitterSystem[In <: Chan[Message], OutA <: Chan[MsgA], OutB <: Chan[MsgB]] =
        Par4[
            Producer[Message, In],
            Splitter[In, OutA, OutB],
            Consumer[MsgA, OutA],
            Consumer[MsgB, OutB]
        ]

    @verify(property = "deadlock_free()") // Success: deadlock-free
    private def splitterSystemCHK(in: Chan[Message],
                            outA: Chan[MsgA],
                            outB: Chan[MsgB]): SplitterSystem[in.type,
                                                              outA.type,
                                                              outB.type] = ???

    type FaultySplitterSystem[In <: Chan[Message], OutA <: Chan[MsgA], OutB <: Chan[MsgB]] =
        Par3[
            Producer[Message, In],
            Splitter[In, OutA, OutB],
            Consumer[MsgA, OutA],
            // No consumer for MsgB
            // Consumer[MsgB, OutB]
        ]

    @verify(property = "deadlock_free()") // Expect failure: can deadlock
    private def splitterDeadlockCHK(in: Chan[Message],
                                    outA: Chan[MsgA],
                                    outB: Chan[MsgB]): FaultySplitterSystem[in.type,
                                                                            outA.type,
                                                                            outB.type] = ???

    // Merger

    type Merger[InA <: IChan[MsgA], InB <: IChan[MsgB], OutC <: OChan[Message]] =
        Rec[RecX,
            Branch[Message, (InA, InB), Tuple1[
                (m: Message) => Out[OutC, m.type] >>: Loop[RecX]
            ]]
        ]

    type FaultyForwarder[InC <: Chan[Message], MiddleA <: Chan[MsgA],
                         MiddleB <: Chan[MsgB], OutC <: Chan[Message]] =
        Par4[
            Producer[Message, InC],
            Splitter[InC, MiddleA, MiddleB],
            Merger[MiddleA, MiddleB, OutC],
            Consumer[Message, OutC]
        ]

    @verify(property = "forwarding(in)(out)") // Expect failure: could re-order messages
    def faultyForwarderCHK(in: Chan[Message],
                      middleA: Chan[MsgA],
                      middleB: Chan[MsgB],
                      out: Chan[Message]): FaultyForwarder[in.type,
                                                           middleA.type,
                                                           middleB.type,
                                                           out.type] = ???

    type ControlledProducer[A, C <: OChan[A], Ctrl <: IChan[Unit]] =
        Rec[RecX,
            Out[C, A] >>: In[Ctrl, Unit, (x: Unit) => Loop[RecX]]
        ]
    
    type ConsumerWithControl[A, C <: IChan[A], Ctrl <: OChan[Unit]] =
        Rec[RecX,
            In[C, A, (x: A) => Out[Ctrl, Unit] >>: Loop[RecX]]
        ]

    type Forwarder[InC <: IChan[Message],
                   MiddleA <: Chan[MsgA],
                   MiddleB <: Chan[MsgB],
                   OutC <: OChan[Message],
                   Ctrl <: Chan[Unit]] =
        Par[
            Rec[RecX,
                Branch1[Message, InC, (
                    (m: MsgA) => Out[MiddleA, m.type] >>: In[Ctrl, Unit, (x: Unit) => Loop[RecX]],
                    (m: MsgB) => Out[MiddleB, m.type] >>: In[Ctrl, Unit, (x: Unit) => Loop[RecX]]
                )]
            ],
            Rec[RecY,
                Branch[Message, (MiddleA, MiddleB), Tuple1[
                    (m: Message) => Out[OutC, m.type] >>: Out[Ctrl, Unit] >>: Loop[RecY]
                ]]
            ]
        ]

    // @verify(property = "forwarding(in)(out)(middleA, middleB)") // Success: preserves message order
    def forwarderCHK(in: IChan[Message],
                     middleA: Chan[MsgA],
                     middleB: Chan[MsgB],
                     out: OChan[Message],
                     ctrl: Chan[Unit]): Forwarder[in.type,
                                                  middleA.type,
                                                  middleB.type,
                                                  out.type,
                                                  ctrl.type] = ???

    // Simpler examples

    type SimpleForwarder[InC <: IChan[Int], OutC <: OChan[Int]] =
        Branch1[Int, InC, Tuple1[
            (m: Int) => Out[OutC, m.type]
        ]]

    @verify(property = "forwarding(in)(out)") // Success
    def simpleForwarderCHK(in: IChan[Int],
                           out: OChan[Int]): SimpleForwarder[in.type,
                                                              out.type] = ???

    type SimpleForwarder2[InC <: IChan[Int | String], OutC <: OChan[Int | String]] =
        Branch1[Int | String, InC, (
            (m: Int) => Out[OutC, m.type],
            (m: String) => Out[OutC, m.type]
        )]

    @verify(property = "forwarding(in)(out)") // Success
    def simpleForwarder2CHK(in: IChan[Int | String],
                            out: OChan[Int | String]): SimpleForwarder2[in.type,
                                                                       out.type] = ???

    type SimpleForwarder3[InC <: IChan[Int | String], OutC <: OChan[Int | String]] =
        Rec[RecX, 
            Branch1[Int | String, InC, (
                (m: Int) => Out[OutC, m.type] >>: Loop[RecX],
                (m: String) => Out[OutC, m.type] >>: Loop[RecX]
            )]
        ]

    @verify(property = "forwarding(in)(out)") // Success
    def simpleForwarder3CHK(in: IChan[Int | String],
                            out: OChan[Int | String]): SimpleForwarder3[in.type,
                                                                        out.type] = ???

    type SimpleForwarder4[InC <: IChan[Message], OutC <: OChan[Message]] =
        Rec[RecX,
            Branch1[Message, InC, (
                (m: MsgA) => Out[OutC, m.type] >>: Loop[RecX],
                (m: MsgB) => Out[OutC, m.type] >>: Loop[RecX]
            )]
        ]

    @verify(property = "forwarding(in)(out)") // Success
    def simpleForwarder4CHK(in: IChan[Message],
                            out: OChan[Message]): SimpleForwarder4[in.type,
                                                                     out.type] = ???

    type SimpleForwarder5[InC <: IChan[Int], MiddleC <: Chan[Int], OutC <: OChan[Int]] =
        Par[
            SimpleForwarder[InC, MiddleC],
            SimpleForwarder[MiddleC, OutC]
        ]

    @verify(property = "forwarding(in)(out)") // Success
    def simpleForwarder5CHK(in: IChan[Int],
                            middle: Chan[Int],
                            out: OChan[Int]): SimpleForwarder5[in.type,
                                                                middle.type,
                                                                out.type] = ???

    type SimpleForwarder6[InC <: IChan[Int], OutC <: OChan[Int]] =
        Branch1[Int | String, InC, (
            (m: Int) => Out[OutC, m.type],
            (m: String) => PNil
        )]

    @verify(property = "forwarding(in)(out)") // Success
    def simpleForwarder6CHK(in: IChan[Int],
                            out: OChan[Int]): SimpleForwarder6[in.type,
                                                               out.type] = ???

    type SimpleForwarder7[InC <: IChan[Message], MiddleC <: Chan[Message], OutC <: OChan[Message]] =
        Par[
            Branch1[Message, InC, (
                (m: MsgA) => Out[MiddleC, m.type],
                (m: MsgB) => Out[MiddleC, m.type]
            )],
            Branch1[Message, MiddleC, (
                (m: MsgA) => Out[OutC, m.type],
                (m: MsgB) => Out[OutC, m.type]
            )]
        ]

    @verify(property = "forwarding(in)(out)") // Success
    def simpleForwarder7CHK(in: IChan[Message],
                            middle: Chan[Message],
                            out: OChan[Message]): SimpleForwarder7[in.type,
                                                                        middle.type,
                                                                        out.type] = ???

    type SimpleForwarder8[InC <: IChan[Message], MiddleA <: Chan[Message], MiddleB <: Chan[Message], OutC <: OChan[Message]] =
        Par3[
            In[InC, Message, (m: Message) => Out[MiddleA, m.type] | Out[MiddleB, m.type]],
            In[MiddleA, Message, (m: Message) => Out[OutC, m.type]],
            In[MiddleB, Message, (m: Message) => Out[OutC, m.type]]
        ]

    @verify(property = "forwarding(in)(out)") // Success
    def simpleForwarder8CHK(in: IChan[Message],
                            middleA: Chan[Message],
                            middleB: Chan[Message],
                            out: OChan[Message]): SimpleForwarder8[in.type,
                                                                     middleA.type,
                                                                     middleB.type,
                                                                     out.type] = ???

    type SimpleForwarder9[InC <: IChan[Message], MiddleA <: Chan[Message], MiddleB <: Chan[Message], OutC <: OChan[Message]] =
        Par[
            Branch1[Message, InC, (
                (m: MsgA) => Out[MiddleA, m.type],
                (m: MsgB) => Out[MiddleA, m.type]
            )],
            Rec[RecX,
                Branch[Message, (MiddleA, MiddleB), (
                    (m: MsgA) => Out[OutC, m.type] >>: Loop[RecX],
                    (m: MsgB) => Out[OutC, m.type] >>: Loop[RecX]
                )]
            ]
        ]

    @verify(property = "forwarding(in)(out)") // FIXME FAILS
    def simpleForwarder9CHK(in: IChan[Message],
                            middleA: Chan[Message],
                            middleB: Chan[Message],
                            out: OChan[Message]): SimpleForwarder9[in.type,
                                                                   middleA.type,
                                                                   middleB.type,
                                                                   out.type] = ???

    type SimpleForwarder10[InA <: IChan[MsgA], InB <: IChan[MsgB], OutA <: OChan[Message], OutB <: OChan[Message]] =
        Branch[Message, (InA, InB), (
            (m: MsgA) => Out[OutA, m.type],
            (m: MsgB) => Out[OutB, m.type]
        )]

    @verify(property = "forwarding(inA, inB)(outA, outB)") // Success
    def simpleForwarder10CHK(inA: IChan[MsgA],
                             inB: IChan[MsgB],
                             outA: OChan[Message],
                             outB: OChan[Message]): SimpleForwarder10[inA.type,
                                                                      inB.type,
                                                                      outA.type,
                                                                      outB.type] = ???

    type SimpleForwarder11[InC <: IChan[Message], MiddleA <: Chan[Message], MiddleB <: Chan[Message], OutC <: OChan[Message]] =
        Par[
            In[InC, Message, (m: Message) => Out[MiddleA, m.type] | Out[MiddleB, m.type]],
            Rec[RecX, Branch[Message, (MiddleA, MiddleB), Tuple1[
                (m: Message) => Out[OutC, m.type] >>: Loop[RecX],
            ]]]
        ]

    @verify(property = "forwarding(in)(out)") // FIXME FAILS
    def simpleForwarder11CHK(in: IChan[Message],
                             middleA: Chan[Message],
                             middleB: Chan[Message],
                             out: OChan[Message]): SimpleForwarder11[in.type,
                                                                     middleA.type,
                                                                     middleB.type,
                                                                     out.type] = ???

    type SimpleForwarder12[InC <: IChan[Message], MiddleA <: Chan[Message], MiddleB <: Chan[Message], OutC <: OChan[Message]] =
        Par3[
            In[InC, Message, (m: Message) => Out[MiddleA, m.type] | Out[MiddleB, m.type]],
            Rec[RecX, In[MiddleA, Message, (m: Message) => Out[OutC, m.type] >>: Loop[RecX]]],
            Rec[RecY, In[MiddleB, Message, (m: Message) => Out[OutC, m.type] >>: Loop[RecY]]]
        ]

    @verify(property = "forwarding(in)(out)") // Successs
    def simpleForwarder12CHK(in: IChan[Message],
                             middleA: Chan[Message],
                             middleB: Chan[Message],
                             out: OChan[Message]): SimpleForwarder12[in.type,
                                                                     middleA.type,
                                                                     middleB.type,
                                                                     out.type] = ???
}