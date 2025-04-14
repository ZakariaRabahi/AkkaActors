package actors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors

import scala.util.Random

object Bidder {
  // Define commands (messages) that the Bidder actor can handle
  sealed trait Command

  case class PlaceBid(newbidAmount: Double) extends Command // Place a new bid
  case class RequestedAuctionList(auctionList: List[(String, ActorRef[Auction.Command])]) extends Command // List of auctions from eBay
  case class CancelBid() extends Command // Cancel an existing bid
  case class BidAccepted(highestBid: Double, item: String, auctionId: String, auctionActor: ActorRef[Auction.Command]) extends Command // Notify bid acceptance
  case class BidRejected(reason: String, item: String) extends Command // Notify bid rejection
  case class CreateBankAccount() extends Command // Request to create a bank account
  case class NotifyNewBid(amount: Double, auction: String) extends Command // Notify about a new highest bid
  case class NotifyCanceledBid(auctionId: String) extends Command // Notify bid cancellation
  case class NotifyCanceledPayment(auctionId: String) extends Command // Notify payment cancellation
  case class NotifySold(replyTo: ActorRef[Auction.Command]) extends Command // Notify that the item has been sold
  case class RefundObject() extends Command // Request a refund
  case class NotifyReAuction(auctionId: String) extends Command // Notify re-auction of an item

  // Variables to store the current bid amount and auctions the bidder is participating in
  var bidAmount: Double = _ // The current bid amount
  var participatedAuctions = Map.empty[String, ActorRef[Auction.Command]] // Auctions the bidder is involved in

  // Define the Bidder's behavior
  def apply(
             bidderId: String,
             bankAccountNumber: String,
             ebayInstance: ActorRef[eBay.Command],
             bankInstance: ActorRef[Bank.Command],
             initialBalance: Double
           ): Behavior[Command] = Behaviors.setup { context =>
    Behaviors.receiveMessage {

      case CreateBankAccount() =>
        // Request the Bank actor to register a new account for the bidder
        bankInstance ! Bank.RegisterAccount(bankAccountNumber, bidderId, initialBalance)
        Behaviors.same

      case PlaceBid(newbidAmount) =>
        // Request the auction list from eBay before placing a bid
        ebayInstance ! eBay.RequestAuctionList(context.self)
        bidAmount = newbidAmount // Store the bid amount
        context.log.info(s"Bidder $bidderId requested auction list from eBay.")
        Behaviors.same

      case RequestedAuctionList(auctionList) =>
        // Randomly select an auction from the list and place the bid
        val randomAuction = Random.shuffle(auctionList).head
        randomAuction._2 ! Auction.PlaceBid(bidAmount, bidderId, bankAccountNumber, context.self)
        context.log.info(s"by $bidderId: new bid of $bidAmount on ${randomAuction._1}.")
        Behaviors.same

      case BidAccepted(highestBid, item, auctionId, auctionActor) =>
        // Update the list of participated auctions and log bid acceptance
        context.log.info(s"$bidderId : your bid has been accepted for item: $item, amount: $highestBid")
        participatedAuctions += auctionId -> auctionActor
        Behaviors.same

      case BidRejected(reason, item) =>
        // Log the reason for bid rejection
        context.log.info(s"$bidderId your bid has been rejected for $item, $reason")
        Behaviors.same

      case NotifyNewBid(amount, auction) =>
        // Notify the bidder about a new highest bid on an auction
        context.log.info(s"notification for $bidderId : there is a new highest bid for $auction, amount: $amount")
        Behaviors.same

      case NotifyCanceledBid(auctionId) =>
        // Notify the bidder that their bid was canceled
        context.log.info(s"notification for $bidderId : your bid for $auctionId has been canceled")
        Behaviors.same

      case NotifyCanceledPayment(auctionId) =>
        // Notify the bidder that their payment was canceled
        context.log.info(s"notification for $bidderId : your Payment for $auctionId has been canceled")
        Behaviors.same

      case CancelBid() =>
        // Randomly cancel a bid from the list of participated auctions
        val randomAuction = Random.shuffle(participatedAuctions).head
        randomAuction._2 ! Auction.CancelBid(bidderId, context.self)
        context.log.info(s"$bidderId : you have canceled your bid on auction: ${randomAuction._1}")
        Behaviors.same

      case NotifySold(replyTo) =>
        // Notify the Auction actor that the bidder has been informed about the sale
        replyTo ! Auction.BidderIsNotified()
        Behaviors.same

      case RefundObject() =>
        // Request a refund from eBay
        ebayInstance ! eBay.AskForRefund(bidderId)
        context.log.info(s"$bidderId wants to place a refund")
        Behaviors.same

      case NotifyReAuction(auctionId) =>
        // Notify the bidder that an auction has been reopened
        context.log.info(s"$bidderId, $auctionId is again available, please place a bid")
        Behaviors.same
    }
  }
}
