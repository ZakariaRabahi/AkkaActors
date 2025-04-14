package actors
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors

object Seller {
  // Define commands (messages) that the Seller actor can handle
  sealed trait Command

  case class CreateAuction(auctionId: String, item: String, initialPrice: Double, availabilityTime: Int, bank: ActorRef[Bank.Command]) extends Command
  // Command to create a new auction

  case class AuctionCreated(auctionId: String) extends Command
  // Notification that the auction has been successfully created

  case class AuctionFailed(auctionId: String, reason: Option[String]) extends Command
  // Notification that the auction creation failed with a reason

  case class EvaluateAuction(auctionId: String, minNumberOfBids: Int, minBidAmount: Double) extends Command
  // Command to evaluate the auction based on certain criteria

  case class RejectEvaluation(auctionId: String, passed: Boolean) extends Command
  // Command to handle the result of the auction evaluation

  case class CancelAuction() extends Command
  // Command to cancel an auction

  case class NotifySold(replyTo: ActorRef[Auction.Command]) extends Command
  // Notify the seller that the item has been sold

  case class ReturnItem(bidderId: String, auctionId: String, amount: Double, replyTo: ActorRef[Auction.Command]) extends Command
  // Command to handle the return of an item

  case class ReAuction() extends Command
  // Command to re-auction an item

  // Keep track of auctions and returned items
  var auctions: List[String] = List.empty // List of auction IDs created by the seller
  var returnedItems = Map.empty[String, ActorRef[Auction.Command]] // Map of returned items to their Auction actors

  // Define the Seller's behavior
  def apply(
             sellerId: String,
             eBayActor: ActorRef[eBay.Command],
             bankActor: ActorRef[Bank.Command]
           ): Behavior[Command] = Behaviors.setup { context =>

    Behaviors.receiveMessage {

      case CreateAuction(auctionId, item, initialPrice, availabilityTime, bank) =>
        // Request eBay to register a new auction
        eBayActor ! eBay.RegisterAuction(auctionId, item, initialPrice, availabilityTime, context.self, bank)
        context.log.info(s"Seller $sellerId asked for registered auction $auctionId for item $item with eBay.")
        auctions = auctions :+ auctionId // Add the auction to the list of created auctions
        Behaviors.same

      case AuctionFailed(auctionId, reason) =>
        // Log the failure of auction creation with the reason
        context.log.info(s"auction $auctionId did not succeed: $reason")
        Behaviors.same

      case AuctionCreated(auctionId) =>
        // Log the success of auction creation
        context.log.info(s"auction $auctionId has been created successfully")
        Behaviors.same

      case EvaluateAuction(auctionId, minNumberOfBids, minBidAmount) =>
        // Request eBay to evaluate the auction
        eBayActor ! eBay.EvaluateAuction(auctionId, minNumberOfBids, minBidAmount, context.self)
        Behaviors.same

      case RejectEvaluation(auctionId, passed) =>
        if (passed) {
          // If the auction failed evaluation, cancel it
          eBayActor ! eBay.CancelAuction(auctionId)
          auctions = auctions.filter(_ != auctionId) // Remove the auction from the list
          context.log.info(s"$auctionId has failed the evaluation, $auctionId will be canceled")
        } else {
          context.log.info(s"$auctionId has passed the evaluation!")
        }
        Behaviors.same

      case CancelAuction() =>
        // Cancel the first auction in the list
        val canceledAuctionId = auctions.head
        eBayActor ! eBay.CancelAuction(canceledAuctionId)
        auctions = auctions.filter(_ != canceledAuctionId) // Remove it from the list
        Behaviors.same

      case NotifySold(replyTo) =>
        // Notify the Auction actor that the seller has been informed about the sale
        replyTo ! Auction.SellerIsNotified()
        Behaviors.same

      case ReturnItem(bidderId, auctionId, amount, replyTo) =>
        // Handle the return of an item and refund the bidder
        returnedItems += auctionId -> replyTo // Add the item to the returned items map
        auctions = auctions.filter(_ != auctionId) // Remove the auction from the list
        context.log.info(s"item from $auctionId has been returned correctly")
        bankActor ! Bank.Refund(bidderId, amount) // Request a refund from the Bank actor
        Behaviors.same

      case ReAuction() =>
        // Handle the re-auctioning of a returned item
        val item = returnedItems.head // Get the first returned item
        val auctionId = item._1
        val auctionActor = item._2
        returnedItems -= auctionId // Remove the item from the returned items map
        auctions = auctions :+ auctionId // Add the auction back to the list
        context.log.info(s"$sellerId asked for a re-auction for $auctionId")
        eBayActor ! eBay.ReAuction(auctionActor) // Notify eBay to re-auction the item
        Behaviors.same
    }
  }
}
