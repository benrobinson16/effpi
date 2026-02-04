package effpi.examples.auctionhouse

import effpi.channel.{Channel => Chan, InChannel => IChan, OutChannel => OChan}
import effpi.process._
import effpi.process.given
import effpi.process.dsl._

package types {
  case class Bid(amount: Int)
  case class CloseAuction()
  case class NewStartingPrice(price: Int)
  case class AuctionEnded()
  type Notification = NewStartingPrice | AuctionEnded
  
  type Auctioneer[C <: OChan[CloseAuction]] =
    Out[C, CloseAuction] >>: PNil

  type Client[C1 <: IChan[Notification], C2 <: OChan[Bid]] =
    Rec[RecX,
      Branch[Notification, Tuple1[C1], (
        (m: NewStartingPrice) => 
          Loop[RecX] | (Out[C2, Bid] >>: Loop[RecX]),
        (m: AuctionEnded) => 
          PNil
      )]
    ]

  type AuctionHouse[ClientChan <: IChan[Bid], AuctioneerChan <: IChan[CloseAuction], BroadcasterChan <: OChan[Notification]] =
    Out[BroadcasterChan, NewStartingPrice] >>: Rec[RecX,
      CatchTimeout[
        Branch[Bid | CloseAuction, (ClientChan, AuctioneerChan), (
          (m: Bid) => Loop[RecX], // FIXME Different continuation if bids exist
          (m: CloseAuction) => Out[BroadcasterChan, AuctionEnded] >>: PNil
        )],
        // If no bids, reduce starting price
        Out[BroadcasterChan, NewStartingPrice] >>: Loop[RecX]
      ]
    ]
}

package implementation {
  import types._
  import scala.concurrent.duration.Duration

  implicit val timeout: Duration = Duration(100, "seconds")

  def auctioneer(c: OChan[CloseAuction]): Auctioneer[c.type] = {
    Thread.sleep(30_000) // Auction runs for 30 seconds
    send(c, CloseAuction()) >> nil
  }

  def client(i: Int, c1: IChan[NewStartingPrice | AuctionEnded], c2: OChan[Bid]): Client[c1.type, c2.type] = {
    val myBidAmount = scala.util.Random.nextInt(100)

    rec(RecX) {
      branch1(c1, (
        (m: NewStartingPrice) => {
          if (m.price <= myBidAmount) {
            println(s"Client ${i}: Bidding ${myBidAmount}")
            send(c2, Bid(myBidAmount)) >> loop(RecX)
          } else {
            println(s"Client ${i}: Not bidding")
            loop(RecX)
          }
        },
        (m: AuctionEnded) => {
          println(s"Client ${i}: Auction ended.")
          nil
        }
      ))
    }
  }

  def broadcaster(c: IChan[Notification],
                  c1: OChan[Notification],
                  c2: OChan[Notification],
                  c3: OChan[Notification]) = {
    rec(RecX) {
      branch1(c, (
        (m: NewStartingPrice) => {
          println(s"Broadcaster: New starting price is ${m.price}, broadcasting to clients.")
          send(c1, m) >> send(c2, m) >> send(c3, m) >> loop(RecX)
        },
        (m: AuctionEnded) => {
          println(s"Broadcaster: Auction ended, notifying clients.")
          send(c1, m) >> send(c2, m) >> send(c3, m) >> nil
        }
      ))
    }
  }

  def auctionHouse(cClient: IChan[Bid], cAuctioneer: IChan[CloseAuction], cBroadcaster: OChan[Notification]): AuctionHouse[cClient.type, cAuctioneer.type, cBroadcaster.type] = {
    var startingPrice = 100

    implicit val timeout: Duration = Duration(3, "seconds") // 3 seconds for bids

    send(cBroadcaster, NewStartingPrice(startingPrice))
      >> rec(RecX) {
        catchTimeout(
          branch((cClient, cAuctioneer), (
            (m: Bid) => {
              println(s"AuctionHouse: Received bid of ${m.amount}, waiting for more bids or auction close.")
              loop(RecX)
            },
            (m: CloseAuction) => {
              println(s"AuctionHouse: Auction closed by auctioneer.")
              send(cBroadcaster, AuctionEnded()) >> nil
            }
          )),
          {
            startingPrice -= 10
            println(s"AuctionHouse: No bids received, reducing starting price to ${startingPrice}.")
            send(cBroadcaster, NewStartingPrice(startingPrice)) >> loop(RecX)
          }
        )
      }
  }
}

// sbt "examples/runMain effpi.examples.auctionhouse.Main"
object Main {
  import types._
  import implementation._
  def main(): Unit = main(Array())

  def main(args: Array[String]) = {
    val client1 = Chan[Notification]()
    val client2 = Chan[Notification]()
    val client3 = Chan[Notification]()
    val broadcastChan = Chan[Notification]()
    val auctioneerChan = Chan[CloseAuction]()
    val bidChan = Chan[Bid]()

    eval(
      par(
        auctioneer(auctioneerChan),
        client(1, client1, bidChan),
        client(2, client2, bidChan),
        client(3, client3, bidChan),
        auctionHouse(bidChan, auctioneerChan, broadcastChan),
        broadcaster(broadcastChan, client1, client2, client3)
      )
    )
  }
}
