package actors
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import scala.concurrent.duration._

object Bank {
  // Define commands (messages) that the Bank actor can handle
  sealed trait Command
  case class RegisterAccount(accountNumber: String, name: String, initialBalance: Double) extends Command
  case class ValidatePayment(bidderId: String, accountNumber: String, amount: Double, replyTo: ActorRef[Auction.Command]) extends Command
  case class Refund(bidderId: String, amount: Double) extends Command

  // Define the state for an account (account number and balance)
  case class Account(accountNumber: String, balance: Double)

  // Main behavior of the Bank actor
  def apply(): Behavior[Command] = Behaviors.withTimers { timers =>
    // Store accounts in a map (key = bidder ID, value = account info)
    var accounts = Map.empty[String, Account]

    Behaviors.setup { context =>
      Behaviors.receiveMessage {

        // Handle account registration
        case RegisterAccount(accountNumber, name, initialBalance) =>
          // Add a new account to the map
          accounts += name -> Account(accountNumber, initialBalance)
          context.log.info(s"Account registered: $accountNumber for $name with initial balance $initialBalance")
          Behaviors.same // Stay in the same behavior

        // Handle payment validation
        case ValidatePayment(bidderId, accountNumber, amount, replyTo) =>
          accounts.get(bidderId) match {
            case Some(account) if account.balance >= amount =>
              // Deduct the amount and update the account balance
              accounts += bidderId -> Account(accountNumber, account.balance - amount)
              context.log.info(s"Payment of $amount validated for account $accountNumber. New balance: ${account.balance - amount}")
              // Notify the Auction actor about successful payment
              replyTo ! Auction.NotifyBidderAndSeller(bidderId)

            case Some(_) =>
              // Insufficient funds in the account
              context.log.info(s"Payment of $amount failed: Insufficient funds for account $accountNumber")
              replyTo ! Auction.NotifyCanceledPayment() // Notify Auction about failure

            case None =>
              // Account not found
              context.log.info(s"Payment failed: Account $accountNumber not found")
              replyTo ! Auction.NotifyCanceledPayment() // Notify Auction about failure
          }
          Behaviors.same // Stay in the same behavior

        // Handle refund requests
        case Refund(bidderId, amount) =>
          accounts.get(bidderId) match {
            case Some(account) =>
              // Add the refund amount to the account balance
              accounts = accounts + (account.accountNumber -> account.copy(balance = account.balance + amount))
              context.log.info(s"Refund of $amount validated for account ${account.accountNumber}. New balance: ${account.balance + amount}")

            case None =>
              // Account not found, refund failed
              context.log.info(s"Refund failed")
          }
          Behaviors.same // Stay in the same behavior
      }
    }
  }
}
