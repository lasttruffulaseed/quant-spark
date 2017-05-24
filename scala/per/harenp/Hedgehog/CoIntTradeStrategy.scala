package per.harenp.Hedgehog

import breeze.numerics.{abs, sqrt}

import scala.collection.Map

/**
 * @author patrick.haren@yahoo.com
 */

class CoIntTradeStrategy(coIntInfo: CoIntegrationSummary, equity: EquityData) extends TradingStrategy {
  /*
   * parent members:
   *   var history : Map[String, PriceData]
   *   def setStockHistories(minuteData : Map[String, PriceData]) = history = minuteData
   */

  var history: Map[String, Array[PriceData]] = Map()
  override def setStockHistories(minuteData : Map[String, Array[PriceData]]) =  { history = minuteData }

  /* parameters for our strategy:
   */
  val txCost = 3.00                                       // assume $3.00/instrument as trade cost
  val synthTxCost = txCost*(coIntInfo.betas.length-1)     // synthetic instrument = a basket => multiple trades for average joe
  val fullTradeCost = txCost+synthTxCost                  // cost to pair trade
  val InThreshold = 1.1*sqrt(coIntInfo.residualVariance)  // per "share" entry threshold (will expect it to mean revert)
  val stopLossRatio = 0.2                                 // break off if position amount dips by more than this ratio
  val OutThreshold = 0.2*InThreshold                      // profit point, after mean reversion

  override def getStocksOfInterest = coIntInfo.contributors.filter(_ != "unity").toList :+ coIntInfo.target

  override def evaluate(startAmt: Double) : StrategyResults = {

    val targetHistory = equity.minutePrices // history(coIntInfo.target)
    val tradePeriods = targetHistory.length

    def syntheticPrice(period: Int) = (0 to coIntInfo.betas.length - 1).map { factor =>

      val beta = coIntInfo.betas(factor)
      val factorName = coIntInfo.contributors(factor)

      if (factorName != "unity" && !history.contains(factorName))
        println("houston: " + factorName)

      val amt = if (factorName == "unity") 1.0 else history(factorName)(period).adjClose

      beta * amt
    }.sum

    var residualAtBuy = 0.0 // residual at entry point
    var positionShares = 0
    var priceAtBuy = 0.0
    var cash = startAmt
    var winners, events = 0

    def longTarget = residualAtBuy < 0 // we are long on target stock when target-synth < 0 (after threshold)
    def timeToBuy(residual: Double) = abs(residual) > InThreshold
    def timeToSell(residual: Double) = if (longTarget) residual > OutThreshold else residual < -OutThreshold
    def gain(residual: Double) = if (longTarget) residual - residualAtBuy else residualAtBuy - residual
    def timeToStopLoss(residual: Double) = gain(residual) < -abs(stopLossRatio * residualAtBuy)
    def haveOpenPosition = abs(residualAtBuy) > 0 // convenience

    def openPosition(residual: Double, targetPrice: Double) = {
      residualAtBuy = residual
      priceAtBuy = targetPrice
      positionShares = ((cash - fullTradeCost) / targetPrice).toInt // just record the actual long counts (approx)
      cash -= (fullTradeCost + positionShares * targetPrice)
    }

    def closePosition(residual: Double) = {
      cash += (gain(residual)+priceAtBuy) * positionShares - fullTradeCost
      winners += (if (gain(residual) > 0) 1 else 0)
      events += 1
      residualAtBuy = 0
      positionShares = 0
      priceAtBuy = 0
    }

    def pairResidual(period: Int) = targetHistory(period).adjClose - syntheticPrice(period) - coIntInfo.residualMean

    // run through the minute-by-minute price data, detecting position signals and performing trades
    for (period <- 0 to tradePeriods - 1) {
      val targetAmt = targetHistory(period).adjClose
      val residual = pairResidual(period)

      //val numInPlay = if (positionShares > 0) positionShares else (cash/targetAmt)
      //val currentGain = numInPlay * (if (amLongOnTarget) residual else -residual)

      if (haveOpenPosition) {
        // looking for when to exit position (think about this one first!)
        if (timeToSell(residual) || timeToStopLoss(residual))
          closePosition(residual)
      }
      else // looking for the start signal {
      if (timeToBuy(residual))
        openPosition(residual, targetAmt)
    }
    StrategyResults(coIntInfo.target, cash + (gain(pairResidual(targetHistory.length-1))+priceAtBuy)*positionShares, events, winners)
  }
}

    /*


    def openPosition(targetPrice : Double) = {
      val buyQuantity = ((cash - fullTradeCost)/targetPrice)
      position = buyQuantity * targetPrice cash - fullTradeCost
      positionShares = position/targetPrice
      cash = 0.0
    }

    def closePosition(currentGainLoss: Double) = {
      if ((currentGainLoss-fullTradeCost)>0) winners+=1 else losers+=1
      cash += position + currentGainLoss - fullTradeCost
      position = 0.0
      positionShares = 0
    }









    var cash = startAmt
    var position = 0.0
    var positionShares = 0
    var amLongOnTarget = false      // holding target (shorting basket) or shorting target (holding basket) ?
    var winners, losers = 0

    // TODO: could make this more generic (to strategies in general)
    def closePosition(currentGainLoss: Double) = {
      if ((currentGainLoss-fullTradeCost)>0) winners+=1 else losers+=1
      cash += position + currentGainLoss - fullTradeCost
      position = 0.0
      positionShares = 0
    }
    def openPosition(targetPrice : Double) = {
      val buyQuantity = ((cash - fullTradeCost)/targetPrice)
      position = buyQuantity * targetPrice cash - fullTradeCost
      positionShares = position/targetPrice
      cash = 0.0
    }

    // run through the minute-by-minute price data, detecting position signals and performing trades
    for (period <- 0 to tradePeriods-1)
    {
      val targetAmt = targetHistory(period).adjClose
      val pairAmt = syntheticPrice(period)
      val residual = abs(targetAmt - pairAmt)
      val numInPlay = if (positionShares > 0) positionShares else (cash/targetAmt)
      val currentGain = numInPlay * (if (amLongOnTarget) residual else -residual)

      if (position > 0)   // looking for when to exit position (think about this one first!)
      {
        if ((currentGain < -stopLossThreshold*position) || (currentGain > OutThreshold))
          closePosition(currentGain)
      }
      else                // looking for the start signal
      {
        if (residual > InThreshold) {
          amLongOnTarget = targetAmt < pairAmt // bet = target will gain on synthetic, or vice-versa
          openPosition(targetAmt)
        }
      }
    }
    StrategyResults(coIntInfo.target, cash+position, winners+losers, winners)

  }

}
*/
