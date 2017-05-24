package per.harenp.Hedgehog

import scala.collection.Map

/**
 * @author patrick.haren@yahoo.com
 *
 * Interface and tester for trading strategies
 */

case class StrategyResults(target: String, endAmt: Double, signals: Int, correctCalls: Int)

abstract class TradingStrategy
{
  // var history : Map[String, Array[PriceData]]
  def getStocksOfInterest : List[String] = ???
  def setStockHistories(minuteData : Map[String, Array[PriceData]]) : Unit = ??? // history = minuteData
  def evaluate(startAmt: Double) : StrategyResults = ???
}

object BackTestFramework {

  def testStrategy(basket: Array[EquityData])(strategy: TradingStrategy) = {

    /**
     * using this function for dependency injection, so that the strategy under back-testing does not need to be aware
     * of running in a Spark cluster vs. local-mode and does not need to know if data is from local files or S3
     */
    def getEquityData(names: List[String]) : Map[String, Array[PriceData]] =
    {
      basket.filter(e => names.contains(e.name)).map(e => (e.name, e.minutePrices)).toMap
    }

    val stocksNeeded = strategy.getStocksOfInterest
    val providedStocks = getEquityData(stocksNeeded)

    strategy.setStockHistories(providedStocks)
    strategy.evaluate(1000000)    // assume a portfolio of $1,000,000 at start
  }

}
