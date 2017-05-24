package per.harenp.Hedgehog

import breeze.linalg._
import breeze.numerics._
import breeze.stats._

/**
  * @author patrick.haren@yahoo.com
  *
  * Processes for co-integrating an equity with an index (or arbitrary basket) of stocks
  */

case class CoIntegrationSummary(target: String, contributors: Array[String], betas: Array[Double], day30VR: Double, residualMean: Double, residualVariance: Double)
{
  def csvReport =
  {
    val summaryRow = "Cointegration report for " + target + ",,, VR=" + day30VR + ",,, Resid.Mean=" + residualMean + ",,, Resid.Var=" + residualVariance
    val headerRow = contributors.reduce(_ + ", " + _)
    val factorsRow = betas.map(_.toString).reduce(_ + ", " + _)
    summaryRow + "\n" + headerRow + "\n" + factorsRow + "\n"
  }
}

object StockCoIntegration {

  /** --------------------------------------------------------------------------------
    * for the stock under test, determine the contribution of each equity to the stock's price movement
    * assumption is that the unknown market factor contributions are present at various betas within the basket of stocks
    * the result of co-integration is a contribution factor for each stock
    */
  def cointegrated(testEquity: EquityData, basket: Array[EquityData], learningDays: Int, validatingDays: Int) =
  {
    // TODO: consider switching to specific dates (ie don't assume that each price array ends on same date)

    val lamda = 0.01 // 0.005 // 0.05

    val totalDataDays = learningDays+validatingDays
    val targetLearningStartPos = testEquity.dailyPrices.length-totalDataDays

    /* Value of a cell in the 'C' matrix (closing prices over learning days[row], for each stock in basket [col])
     * row = values on a particular trade period
     * col = stock# (or constant/unity, if col = 0)
     */
    def CValue(row: Int, col: Int) : Double =
    {
        if (col == 0) 1.0 // col 0 is the offset bias
        else {
          val learningStartPos = basket(col-1).dailyPrices.length - totalDataDays
          basket(col - 1).dailyPrices(learningStartPos + row).adjClose // other cols are for each stock in basket
        }
    }

    /* co-integration formula:
     * inverse(C.transposed*C + lamda*sigma*I)*(C.transposed*t)
     * where C = pricing of each stock, over training days
     * t = pricing of target stock, over same period
     * lamda = ridge coefficient [regularization tuning factor - to balance bias vs. variance]
     * {source: Applied quantitative methods for trading and investment, Ch. 2 (Burgess)}
     */
    val C = DenseMatrix.tabulate(learningDays, basket.length+1)(CValue) // C - the matrix of price at a time (row) per stock (col)
    val target = DenseVector(testEquity.dailyPrices.slice(targetLearningStartPos, targetLearningStartPos+learningDays).map(_.adjClose))
    val Ctransposed = C.t

    if (Ctransposed.cols != target.length)
      println("TODO: houston")

    val Ct : DenseVector[Double] = Ctransposed * target
    val corr : DenseMatrix[Double] = Ctransposed * C
    for (i <- 0 to basket.length) corr(i,i) = (1+lamda) * corr(i,i)        // apply the ridge factor (lamda*sigma*I)
    val invCorr = inv(corr)

    val betas : DenseVector[Double] = invCorr * Ct

    betas.toArray
  }

  /** --------------------------------------------------------------------------------
    * Produce a set of measures for performance of the model.
    * We test the set of factors (betas) over the posterior validation period.
    * The fit of the model - i.e. are the underlying implicit/unknown market factors well represented by
    * the explicit/known stock basket factors.
    * Note: beta(0) is the constant factor, beta(1) is factor for first basket stock, beta(2) is 2nd stock factor, etc.
    * Test for overall mean and variance in test period
    * Test for mean-reversion (for hedging and stat-arb opportunities) using variance ratio
    * TODO: could define a generic Arbitrage Pricing Theory factor-fit measure, where the factor graphs don't have to be
    *  equities. They could be, for example, regular market factors (e.g. price of gold, oil, ForEx, CPI, etc.)
    */
  def fitOfFactorModel(testEquity: EquityData, basket: Array[EquityData], validatingDays: Int, betas: Array[Double]) =
  {
      val daysInSlider = 30 // we'll go with a standard 30 days for mean-reversion test, using variance ratio

      //def factorAmount(factor: Int, day: Int) = if (factor==0) betas(factor) else betas(factor)*baske
      def factorAmount(factor: Int, day: Int) : Double =
      {
        val factorPrice = if (factor==0) 1 else basket(factor-1).dailyPrices.takeRight(validatingDays)(day).adjClose
        betas(factor)*factorPrice
      }

      def sqr(x: Double) = x * x    // basic square function
      // didn't find any nice pre-built sliding window variance function, so here goes...
      def windowVariance(prices: Array[Double], w1Start: Int, w2Start: Int, windowSize: Int) =
        (0 to windowSize-1).map(day => sqr(prices(w1Start+day)-prices(w2Start+day))).sum/(daysInSlider-1)

      // synthetic stock (composed of APT-style factors from each stock)
      val synthStock = (0 to validatingDays-1).map(day => (0 to basket.length).map(b => factorAmount(b, day)).sum).toArray

      // target stock being synthesized
      val targetStock = testEquity.dailyPrices.takeRight(validatingDays).map(_.adjClose)

      // alpha component of pairing the synthetic with target
      val residuals = synthStock.zip(targetStock).map(pair => pair._1 - pair._2)

      val varResiduals = variance(residuals)
      val avgResiduals = mean(residuals)

      val windowSize = validatingDays-daysInSlider-1
      val windowVarianceAtStart = windowVariance(residuals, 0, 1, windowSize)
      val windowVarianceAtEnd   = windowVariance(residuals, 0, daysInSlider-1, windowSize )
      val day30VarianceRatio    = windowVarianceAtEnd/( daysInSlider * windowVarianceAtStart)

      (avgResiduals, varResiduals, day30VarianceRatio)
  }

  // when there's not enough data to analyse
  def NotTried(targetName: String) = CoIntegrationSummary(targetName, Array("unknown"), Array(1.0), 9999.99, 9999.99, 9999.99 )

  /** --------------------------------------------------------------------------------
    * For a particular instrument, find the optimal small basket to pair for stat-arb and hedging strategies
    * This requires 2 co-integration passes:
    *   1. cointegrate with full basket, to get betas
    *   2. pick top n betas (highest weights)
    *   3. cointegrate with the smaller basket, to get final betas
    *   4. produce summary
    *     with name of each "factor" (stock/unity), factor weights, variance ration and other measures
    */
  def optimalSmallBasket(testEquity: EquityData, basket: Array[EquityData], learningDays: Int, validatingDays: Int, smallSize: Int) =
  {
    // 0. ---------------------------------
    // We skip co-integration if the learning and validation days are larger than length that equity has data
    // We remove the target equity from the basket, if present (no sense in co-integrating with itself!)
    if (testEquity.dailyPrices.length < (learningDays+validatingDays) ) NotTried(testEquity.name)
    else {
      val testBasket = basket.filter(_.name != testEquity.name)

      // 1. ---------------------------------
      val fullBetas = cointegrated(testEquity, testBasket, learningDays, validatingDays)

      // 2. ---------------------------------
      val indexedFactors = fullBetas.tail.zipWithIndex // we'll recompute constant factor with new basket => ignored
      val sortedFactors = indexedFactors.sortBy(elem => -abs(elem._1))

      // 3. ---------------------------------
      val smallBasket = (0 to smallSize - 1).map(w => testBasket(sortedFactors(w)._2)).toArray // top contributors
      val betas = cointegrated(testEquity, smallBasket, learningDays, validatingDays)

      // 4. ---------------------------------
      val fit = fitOfFactorModel(testEquity, smallBasket, validatingDays, betas)
      val factorNames = Array("unity") ++ smallBasket.map(_.name)

      CoIntegrationSummary(testEquity.name, factorNames, betas, fit._3, fit._1, fit._2)
    }
  }
}
