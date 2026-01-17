
package effpi.examples.replychanverification

import effpi.channel.{Channel => Chan, InChannel => IChan, OutChannel => OChan}
import effpi.process._
import effpi.process.dsl._
import effpi.verifier.verify

package object types {
    type Doubler[C <: IChan[(OChan[Int], Int)]] =
        Rec[RecX,
            In[C, (OChan[Int], Int), (msg: (OChan[Int], Int)) =>
                Out[msg._1.type, Int] >>: Loop[RecX]
            ]
        ]

    type Client[C <: OChan[(OChan[Int], Int)], C2 <: Chan[Int]] =
        Rec[RecY,
            Out[C, (C2, Int)] >>:
                In[C2, Int, (x: Int) => Loop[RecY]]
        ]


    type DoublerSystem[C1 <: Chan[(OChan[Int], Int)],
                       C2 <: Chan[Int]] =
        Par[
            Doubler[C1],
            Client[C1, C2]
        ]
        

    @verify(property = "deadlock_free()")
    def doublerCHK(c: Chan[(OChan[Int], Int)], r: Chan[Int]): DoublerSystem[c.type, r.type] = ???

    type Replier[C <: IChan[OChan[Unit]]] =
        Rec[RecX,
            In[C, OChan[Unit], (replyChan: OChan[Unit]) =>
                Out[replyChan.type, Unit] >>: Loop[RecX]
            ]
        ]

    type Client2[C <: OChan[OChan[Unit]], R <: Chan[Unit]] =
        Rec[RecY,
            Out[C, R] >>:
                In[R, Unit, (x: Unit) => Loop[RecY]]
        ]

    type ReplierSystem[C1 <: Chan[OChan[Unit]],
                        C2 <: Chan[Unit]] =
        Par[
            Replier[C1],
            Client2[C1, C2]
        ]

    @verify(property = "deadlock_free()")
    def replierCHK(c: Chan[OChan[Unit]], r: Chan[Unit]): ReplierSystem[c.type, r.type] = ???
}

