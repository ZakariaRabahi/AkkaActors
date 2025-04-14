import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import actors.*

object Main extends App {
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  // Create the Bank and eBay systems
  val bankSystem = ActorSystem(Bank(), "Banksystem") // Actor system for Bank
  val ebaySystem = ActorSystem(eBay(), "eBaySystem") // Actor system for eBay
  Thread.sleep(2000) // Wait for systems to initialize

  // Create Seller systems
  val seller1System = ActorSystem(Seller("seller1", ebaySystem, bankSystem), "Seller1System")
  val seller2System = ActorSystem(Seller("seller2", ebaySystem, bankSystem), "Seller2System")
  val seller3System = ActorSystem(Seller("seller3", ebaySystem, bankSystem), "Seller3System")
  val seller4System = ActorSystem(Seller("seller4", ebaySystem, bankSystem), "Seller4System")
  val seller5System = ActorSystem(Seller("seller5", ebaySystem, bankSystem), "Seller5System")
  Thread.sleep(2000) // Allow sellers to initialize

  // Create Bidder systems
  val bidder1System = ActorSystem(Bidder("bidder1", "acc1", ebaySystem, bankSystem, 35000), "Bidder1System")
  val bidder2System = ActorSystem(Bidder("bidder2", "acc2", ebaySystem, bankSystem, 35000), "Bidder2System")
  val bidder3System = ActorSystem(Bidder("bidder3", "acc3", ebaySystem, bankSystem, 35000), "Bidder3System")
  val bidder4System = ActorSystem(Bidder("bidder4", "acc4", ebaySystem, bankSystem, 35000), "Bidder4System")
  val bidder5System = ActorSystem(Bidder("bidder5", "acc5", ebaySystem, bankSystem, 35000), "Bidder5System")
  val bidder6System = ActorSystem(Bidder("bidder6", "acc6", ebaySystem, bankSystem, 35000), "Bidder6System")
  val bidder7System = ActorSystem(Bidder("bidder7", "acc7", ebaySystem, bankSystem, 35000), "Bidder7System")
  val bidder8System = ActorSystem(Bidder("bidder8", "acc8", ebaySystem, bankSystem, 35000), "Bidder8System")
  val bidder9System = ActorSystem(Bidder("bidder9", "acc9", ebaySystem, bankSystem, 35000), "Bidder9System")
  val bidder10System = ActorSystem(Bidder("bidder10", "acc10", ebaySystem, bankSystem, 35000), "Bidder10System")
  Thread.sleep(2000) // Allow bidders to initialize

  // Sellers create auctions
  seller1System ! Seller.CreateAuction("auction1", "Laptop1", 300.0, 10000, bankSystem)
  seller2System ! Seller.CreateAuction("auction2", "auto2", 300.0, 10000, bankSystem)
  seller3System ! Seller.CreateAuction("auction3", "gsm3", 300.0, 10000, bankSystem)
  seller4System ! Seller.CreateAuction("auction4", "micro4", 300.0, 10000, bankSystem)
  seller5System ! Seller.CreateAuction("auction5", "lamp5", 300.0, 10000, bankSystem)
  Thread.sleep(2000) // Wait for auctions to be created

  // Bidders create bank accounts
  bidder1System ! Bidder.CreateBankAccount()
  bidder2System ! Bidder.CreateBankAccount()
  bidder3System ! Bidder.CreateBankAccount()
  bidder4System ! Bidder.CreateBankAccount()
  bidder5System ! Bidder.CreateBankAccount()
  bidder6System ! Bidder.CreateBankAccount()
  bidder7System ! Bidder.CreateBankAccount()
  bidder8System ! Bidder.CreateBankAccount()
  bidder9System ! Bidder.CreateBankAccount()
  bidder10System ! Bidder.CreateBankAccount()

  Thread.sleep(2000) // Wait for accounts to be created

  // Bidders place bids on auctions
  bidder1System ! Bidder.PlaceBid(350.0)
  Thread.sleep(300)
  bidder2System ! Bidder.PlaceBid(450.0)
  Thread.sleep(300)
  bidder3System ! Bidder.PlaceBid(550.0)
  Thread.sleep(300)
  bidder4System ! Bidder.PlaceBid(570.0)
  Thread.sleep(300)
  bidder5System ! Bidder.PlaceBid(725.0)
  Thread.sleep(300)
  bidder6System ! Bidder.PlaceBid(200.0)
  Thread.sleep(300)
  bidder7System ! Bidder.PlaceBid(150.0)
  Thread.sleep(300)
  bidder8System ! Bidder.PlaceBid(650.0)
  Thread.sleep(300)
  bidder9System ! Bidder.PlaceBid(560.0)
  Thread.sleep(300)
  bidder10System ! Bidder.PlaceBid(730.0)
  Thread.sleep(1000)

  // Request a list of auctions and their bids from eBay
  ebaySystem ! eBay.RequestAuctionAndBidsList()
  Thread.sleep(9000) // Allow time for results to process

  // Bidder requests a refund and seller re-auctions the item
  bidder5System ! Bidder.RefundObject()
  Thread.sleep(2000)
  seller1System ! Seller.ReAuction()

  Thread.sleep(9000) // Allow time for re-auction

  // Terminate all systems
  bankSystem.terminate()
  ebaySystem.terminate()
  seller1System.terminate()
}
