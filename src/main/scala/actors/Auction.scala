package actors
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import scala.concurrent.duration._

object Auction {
  // Define commands (messages) that the Auction actor can handle
  sealed trait Command

  case class PlaceBid(amount: Double, bidderId: String, bankAccountNumber: String, replyTo: ActorRef[Bidder.Command]) extends Command
  // Command for placing a bid on the auction

  case class AuctionTimeExpired() extends Command
  // Command triggered when the auction time expires

  case class CancelBid(bidderId: String, replyTo: ActorRef[Bidder.Command]) extends Command
  // Command to cancel a bid

  case class UpdateStatus() extends Command
  // Command to update the auction's status (e.g., sold or not)

  case class NotifyCanceledPayment() extends Command
  // Command to notify the auction about a canceled payment

  case class Evaluate(auctionId: String, minNumberOfBids: Int, minBidAmount: Double, replyTo: ActorRef[Seller.Command]) extends Command
  // Command to evaluate the auction's success based on bids

  case class NotifyBidderAndSeller(bidderId: String) extends Command
  // Command to notify the highest bidder and seller

  case class SellerIsNotified() extends Command
  // Command to confirm that the seller has been notified

  case class BidderIsNotified() extends Command
  // Command to confirm that the bidder has been notified

  case class AskForRefund() extends Command
  // Command to handle refund requests

  case class ReAuction(replyTo: ActorRef[eBay.Command]) extends Command
  // Command to restart the auction

  // A case class to represent a bid
  case class Bid(amount: Double, bidderId: String, bankAccountNumber: String, bidderActor: ActorRef[Bidder.Command])

  // Main behavior of the Auction actor
  def apply(
             auctionId: String,
             item: String,
             initialPrice: Double,
             availabilityTime: Int,
             eBayActor: ActorRef[eBay.Command],
             bankActor: ActorRef[Bank.Command],
             sellerActor: ActorRef[Seller.Command]
           ): Behavior[Command] = Behaviors.setup { context =>
    // Variables to store auction state
    var bids = List.empty[Bid] // List of all bids placed
    var AcceptBids = true // Whether the auction is accepting bids
    var sold = false // Whether the item is sold
    var BidderNotified = false // Whether the bidder has been notified
    var SellerNotified = false // Whether the seller has been notified
    var GracePeriodStarted = false // Whether the grace period has started

    // Schedule the auction expiration based on availability time
    context.scheduleOnce(availabilityTime.milliseconds, context.self, AuctionTimeExpired())

    Behaviors.receiveMessage {
      case PlaceBid(amount, bidderId, bankAccountNumber, replyTo) =>
        // Handle a bid being placed
        val previousHighestBid = bids.headOption.map(_.amount).getOrElse(0.0)
        if (!AcceptBids) {
          replyTo ! Bidder.BidRejected("Auction is expired, no more bids accepted", item)
        } else if (amount > initialPrice && amount > previousHighestBid) {
          // Add the new bid to the list and sort by bid amount
          bids = (Bid(amount, bidderId, bankAccountNumber, replyTo) :: bids).sortBy(-_.amount)
          replyTo ! Bidder.BidAccepted(amount, item, auctionId, context.self)
          eBayActor ! eBay.UpdateHighestBid(auctionId, amount)

          // Notify other bidders about the new highest bid
          bids.foreach(bid =>
            if (bid.bidderActor != replyTo) {
              bid.bidderActor ! Bidder.NotifyNewBid(amount, auctionId)
            }
          )
        } else {
          replyTo ! Bidder.BidRejected("Bid amount too low", item)
        }
        Behaviors.same

      case AuctionTimeExpired() =>
        // Handle the auction expiration
        AcceptBids = false
        context.log.info(s"Time for $auctionId is expired, no more bids accepted")

        if (bids.nonEmpty) {
          val winningBid = bids.head
          bankActor ! Bank.ValidatePayment(winningBid.bidderId, winningBid.bankAccountNumber, winningBid.amount, context.self)
          context.log.info(s"$auctionId has contacted the bank for payment validation")
        } else {
          context.log.info(s"$auctionId has been closed with no bidders!")
        }
        Behaviors.same

      case Evaluate(auctionId, minNumberOfBids, minBidAmount, replyTo) =>
        // Evaluate the auction based on the number of bids and bid amounts
        if (bids.size < minNumberOfBids || bids.headOption.map(_.amount).getOrElse(0.0) < minBidAmount) {
          replyTo ! Seller.RejectEvaluation(auctionId, passed = true)
        } else {
          replyTo ! Seller.RejectEvaluation(auctionId, passed = false)
        }
        Behaviors.same

      case CancelBid(bidderId, replyTo) =>
        // Handle bid cancellation
        val previousHighestBidder = bids.headOption.map(_.bidderId)
        bids = bids.filterNot(_.bidderId == bidderId)
        bids = bids.sortBy(-_.amount)
        replyTo ! Bidder.NotifyCanceledBid(auctionId)
        if (bidderId == previousHighestBidder.getOrElse("")) {
          eBayActor ! eBay.UpdateHighestBid(auctionId, bids.headOption.map(_.amount).getOrElse(0.0))
        }
        Behaviors.same

      case NotifyCanceledPayment() =>
        // Handle notification of payment cancellation
        val nextBid = bids.headOption.map(_.bidderActor).get
        nextBid ! Bidder.NotifyCanceledPayment(auctionId)
        bids = bids.tail // Remove the canceled bid
        Behaviors.same

      case NotifyBidderAndSeller(bidderId) =>
        // Notify both the highest bidder and seller
        val winningBid = bids.find(_.bidderId == bidderId).get
        winningBid.bidderActor ! Bidder.NotifySold(context.self)
        sellerActor ! Seller.NotifySold(context.self)
        Behaviors.same

      case SellerIsNotified() =>
        // Mark the seller as notified
        SellerNotified = true
        context.self ! UpdateStatus()
        Behaviors.same

      case BidderIsNotified() =>
        // Mark the bidder as notified
        BidderNotified = true
        context.self ! UpdateStatus()
        Behaviors.same

      case UpdateStatus() =>
        // Update the auction's status to sold if both parties are notified
        if (BidderNotified && SellerNotified) {
          sold = true
          if (!GracePeriodStarted) {
            GracePeriodStarted = true
            eBayActor ! eBay.StartGracePeriod(bids.head.bidderId, auctionId, context.self)
          }
        }
        Behaviors.same

      case AskForRefund() =>
        // Handle refund request
        val winningBid = bids.headOption.get
        sellerActor ! Seller.ReturnItem(winningBid.bidderId, auctionId, winningBid.amount, context.self)
        Behaviors.same

      case ReAuction(replyTo) =>
        // Restart the auction
        replyTo ! eBay.ReAuctionInfo(auctionId, item, initialPrice, context.self)
        bids = List.empty
        AcceptBids = true
        sold = false
        context.scheduleOnce(availabilityTime.milliseconds, context.self, AuctionTimeExpired())
        Behaviors.same
    }
  }
}
