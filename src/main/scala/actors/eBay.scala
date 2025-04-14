package actors
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import scala.util.Random
import akka.util.Timeout
import scala.concurrent.duration._

object eBay {
  // Define commands (messages) that the eBay actor can handle
  sealed trait Command

  case class RegisterAuction(auctionId: String, item: String, initialPrice: Double, availabilityTime: Int,
                             replyTo: ActorRef[Seller.Command], bank: ActorRef[Bank.Command]) extends Command
  // Command to register a new auction

  case class EvaluateAuction(auctionId: String, numberOfBids: Int, bidAmount: Double, replyTo: ActorRef[Seller.Command]) extends Command
  // Command to evaluate an auction based on criteria

  case class RequestAuctionList(replyTo: ActorRef[Bidder.Command]) extends Command
  // Command to request the list of active auctions

  case class UpdateHighestBid(auctionId: String, bidAmount: Double) extends Command
  // Command to update the highest bid for an auction

  case class CancelAuction(auctionId: String) extends Command
  // Command to cancel an auction

  case class RequestAuctionAndBidsList() extends Command
  // Command to request the list of auctions along with their bids

  case class StartGracePeriod(bidderId: String, auctionId: String, auctionActor: ActorRef[Auction.Command]) extends Command
  // Command to start the grace period for a purchase

  case class GraceTimeExpired(bidderId: String, auctionId: String) extends Command
  // Command triggered when the grace period expires

  case class AskForRefund(bidderId: String) extends Command
  // Command to request a refund

  case class ReAuction(auctionActor: ActorRef[Auction.Command]) extends Command
  // Command to restart an auction

  case class ReAuctionInfo(auctionId: String, item: String, initialPrice: Double, replyTo: ActorRef[Auction.Command]) extends Command
  // Command to provide details about a re-auction

  // Store information about auctions, bids, and items
  var auctions = Map.empty[String, ActorRef[Auction.Command]] // Map of auction IDs to their actors
  var auctionBids = Map.empty[String, Double] // Map of auction IDs to their highest bids
  var auctionItems = Map.empty[String, String] // Map of auction IDs to item descriptions
  var gracePeriods = Map.empty[String, ActorRef[Auction.Command]] // Map of bidder IDs to auctions in grace period

  // Define the eBay actor behavior
  def apply(): Behavior[Command] = Behaviors.setup { context =>

    Behaviors.receiveMessage {
      case RegisterAuction(auctionId, item, initialPrice, availabilityTime, replyTo, bank) =>
        // Register a new auction, creating an Auction actor
        if (auctions.contains(auctionId)) {
          replyTo ! Seller.AuctionFailed(auctionId, Some("Auction name already in use"))
        } else {
          val AuctionActor = context.spawn(
            Auction(auctionId, item, initialPrice, availabilityTime, context.self, bank, replyTo),
            s"auction-$auctionId"
          )
          auctions += auctionId -> AuctionActor
          auctionBids += auctionId -> initialPrice
          auctionItems += auctionId -> item
          replyTo ! Seller.AuctionCreated(auctionId)
        }
        Behaviors.same

      case RequestAuctionList(replyTo) =>
        // Send the list of active auctions to the requester
        val auctionActors: List[(String, ActorRef[Auction.Command])] = auctions.toList
        if (auctionActors.nonEmpty) {
          replyTo ! Bidder.RequestedAuctionList(auctionActors)
          context.log.info("Auction List has been sent")
        } else {
          context.log.info("Auction List is empty, please create an auction first")
        }
        Behaviors.same

      case RequestAuctionAndBidsList() =>
        // Print details of all active auctions and their highest bids
        auctions.keys.foreach { key =>
          val bid = auctionBids.get(key)
          val item = auctionItems.get(key)
          println(s"Key: ${key}, Bid: $bid, Item: $item")
        }
        Behaviors.same

      case UpdateHighestBid(auctionId, bidAmount) =>
        // Update the highest bid for a specific auction
        if (auctionBids.contains(auctionId)) {
          auctionBids += auctionId -> bidAmount
          context.log.info(s"The highest bid for $auctionId is now $bidAmount")
        } else {
          context.log.warn(s"Unknown auction ID: $auctionId")
        }
        Behaviors.same

      case EvaluateAuction(auctionId, numberOfBids, bidAmount, replyTo) =>
        // Evaluate an auction by sending the evaluation message to its actor
        val evaluatedAuction = auctions(auctionId)
        evaluatedAuction ! Auction.Evaluate(auctionId, numberOfBids, bidAmount, replyTo)
        Behaviors.same

      case CancelAuction(auctionId) =>
        // Cancel an auction and remove it from the active list
        val removedAuction = auctions(auctionId)
        removedAuction ! Auction.NotifyCanceledPayment()
        auctions -= auctionId
        auctionBids -= auctionId
        auctionItems -= auctionId
        Behaviors.same

      case StartGracePeriod(bidderId, auctionId, auctionActor) =>
        // Start the grace period for an auction
        gracePeriods += bidderId -> auctionActor
        context.log.info(s"Grace period for $auctionId has started")
        auctions -= auctionId
        auctionBids -= auctionId
        auctionItems -= auctionId
        context.scheduleOnce(30.seconds, context.self, GraceTimeExpired(bidderId, auctionId))
        Behaviors.same

      case GraceTimeExpired(bidderId, auctionId) =>
        // Handle the expiration of the grace period
        if (gracePeriods.contains(bidderId)) {
          gracePeriods -= bidderId
          context.log.info(s"Grace period for $auctionId has expired")
        }
        Behaviors.same

      case AskForRefund(bidderId) =>
        // Forward a refund request to the relevant auction
        if (gracePeriods.contains(bidderId)) {
          val auctionActor = gracePeriods(bidderId)
          auctionActor ! Auction.AskForRefund()
          context.log.info(s"Refund requested for bidder $bidderId")
        } else {
          context.log.info(s"Refund rejected for bidder $bidderId (too late)")
        }
        Behaviors.same

      case ReAuction(auctionActor) =>
        // Restart an auction by notifying its actor
        auctionActor ! Auction.ReAuction(context.self)
        Behaviors.same

      case ReAuctionInfo(auctionId, item, initialPrice, replyTo) =>
        // Add the re-auction to the active auctions
        context.log.info(s"$auctionId is available again")
        auctions += auctionId -> replyTo
        auctionBids += auctionId -> initialPrice
        auctionItems += auctionId -> item
        Behaviors.same
    }
  }
}
