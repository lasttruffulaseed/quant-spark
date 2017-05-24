/**
 * @author patrick.haren@yahoo.com
 *
 */

import java.time.{Instant, LocalDate}
import java.io._

import breeze.numerics.abs
import org.apache.spark.rdd.RDD
import per.harenp.Hedgehog._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.Map

/**
 * Price analysis using Spark, with files on S3
 * all directories are under the S3 bucket
 * args(0) is AWS key Id
 * args(1) is AWS access string
 * args(2) is directory for minute-by-minute price files - "raw-minute-data" or "sample-mins"
 * args(3) is directory for daily-historical price files - "raw-daily-data" or "sample-days"
 * args(4) is basket of stock names to use (i.e. index) - "spx-symbols.txt" or "spx-sample.txt"
 * args(5) is the output directory location for interesting results
 */
object MarketsSpark extends App
 {
    val conf = new SparkConf().
    setSparkHome("/Users/harenp/development/spark").
    setAppName("Market Data App").
    setMaster("local[4]")  // try 4 cores for now

    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId",args(0))
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey",args(1))

    val reportDir = args(5)

    val wallStart = Instant.now.toEpochMilli    // timer

    /* -------------------------------------------------------------------
     * prepare RDD of equity data for all stocks with data
     */
    val dbBase = "/Users/harenp/localed/ub-ms/cs597-project/dev-base/project/"
    // val dbBase = "s3n://hedgehog-markets/"
    val minsDir = dbBase + args(2)
    val daysDir = dbBase + args(3)

    val firstDate = LocalDate.parse("2015-02-12")

    val equities = PriceDataMunging.equitiesOnSpark(firstDate, minsDir, daysDir, sc).persist()

    /* -------------------------------------------------------------------
     * prepare a basket of stocks - e.g. comprising the S&P 500 equity index
     */
    // load symbols making up the index from s3 file
    val basketSymbols = sc.wholeTextFiles(dbBase + args(4)).map(pr=>pr._2).first().lines.toArray

    basketSymbols.foreach(println)
    val b = basketSymbols.contains("ACT")
    println(b)

    println("---")

    val basket = equities.filter(e => basketSymbols.contains(e.name)).collect()   // collect

    // share the full basket index, so that it is available to each worker w/o a full SeDe each time
    val commonBasket = sc.broadcast(basket)

    /* -------------------------------------------------------------------
     * do the heavy lifting !
     * for each equity, perform co-integration with the index basket
     * then co-integrate again, using the top 5 "factors" that have the highest weight
     * record the weights and measures in summaries (collected from the distributed computation)
     */

    // perform the co-integration analysis for each equity
    //val summaries = equities.map(target => StockCoIntegration.optimalSmallBasket(target, commonBasket.value, 1000, 200, 5)).collect()
    //val summaries = equities.filter(_.name == "AME").map(target => StockCoIntegration.optimalSmallBasket(target, commonBasket.value, 1000, 200, 5)).collect()

    val summaries = equities.map(target => StockCoIntegration.optimalSmallBasket(target, commonBasket.value, 1000, 200, 5)).collect()

    // looking for lowest variance ratios - if significantly lower than 1.0, it probably represents a mean-reverting residual
    val sortedSummaries = summaries.sortBy(_.day30VR)

    //
    sortedSummaries.foreach(r => println(r.csvReport))

    println("--------------------------------------") // finished!

    val wallEnd = Instant.now.toEpochMilli

    println("## Total time: - " + (wallEnd-wallStart) + " milliseconds")

    /* -------------------------------------------------------------------
     * compile reports for some of the interesting candidates!
     * note: doing this on the spark client for now...
     * if there are lots of interesting candidates, we can move to workers.
     */

    // sortedSummaries.filter(s => (s.day30VR < 0.5) && (abs(s.residualMean)< 3)).foreach(s => fullReport(s, equities, 1000, reportDir))
    sortedSummaries.filter(_.day30VR < 0.5).foreach(s => fullReport(s, equities, 1000, reportDir))

    /* -------------------------------------------------------------------
     * test the length of each minute-by-minute data set
     * (should all be the same, based on retrieval and interpolation)
     */

    val minAmounts = equities.map(e => (e.name, e.allMinClose.length)).collect()
    val numMins = minAmounts(0)._2

    println("The following equities do not have " + numMins + " minutes (" + numMins/391 + " days) of 60-second data:")
    minAmounts.filter(_._2 != numMins).foreach(e=> println(e._1 + " \t : " + e._2))

    println("--------------------------------------")

    /* -------------------------------------------------------------------
     * backtest the performance of statistical arbitrage pair trading strategy, based on pairs found with best mean-reversion
     */

    val summaryByEquity = sortedSummaries.map(s => (s.target, s)).toMap

    val backTested = equities.filter(e => (summaryByEquity.contains(e.name)) && (summaryByEquity(e.name).day30VR < 0.5)).
      map(e => BackTestFramework.testStrategy(commonBasket.value)(new CoIntTradeStrategy(summaryByEquity(e.name), e))).collect()

    // other thoughts...
    //val backTested = sc.parallelize(sortedSummaries).map(r => BackTestFramework.testStrategy(commonBasket.value)(new CoIntTradeStrategy(r))
    //val backTested = sortedSummaries.map(r => BackTestFramework.testStrategy(commonBasket.value)(new CoIntTradeStrategy(r)))

    backTested.foreach(r => println(r.target, "\t ", r.endAmt, "\t ", r.correctCalls, "\t ",r.signals))


  /**
   * Just defining the report function here, for now...
   * Longer term, should we introduce the RDD concept to StockCointegration, or perhaps isolate using IoC?
   * Also, might need to push to workers, if a lot to report
   */
  def fullReport(summary: CoIntegrationSummary, equities: RDD[EquityData], reportDays: Int, reportDir: String) =
  {
    val equityName = summary.target
    val equitiesByName = equities.
      filter(e => e.name == equityName || summary.contributors.contains(e.name)).
      map(e => (e.name, e)).collectAsMap()
    def factorValue(eqName: String, day: Int) = {
      if (eqName == "unity") 1.0
      else equitiesByName(eqName).dailyPrices.takeRight(reportDays)(day).adjClose
    }
    val target = equitiesByName(equityName)

    val summaryRow = "Cointegration for " + equityName + ". VR=" + summary.day30VR + ". Resid.Mean=" +
      summary.residualMean + ". Resid.Var=" + summary.residualVariance
    val factorsRow = "Factor wts:, " + summary.betas.map(_.toString).reduce(_ + ", " + _)
    val headerRow =  "Date, " + summary.contributors.reduce(_ + ", " + _) + ", " + equityName + ", Synth, Residual"

    println("full report for - " + equityName)

    val pw = new PrintWriter(new File(reportDir + "//" + equityName + "-co-integration.csv"))
    pw.println(summaryRow)
    pw.println(factorsRow)
    pw.println(headerRow)

    (0 to reportDays-1).foreach(
      day => {
        val targetOnDay = target.dailyPrices.takeRight(reportDays)(day)
        pw.print(targetOnDay.nyTradingDate + ", ")
        val rowVals = summary.contributors.map(factorValue(_, day))
        rowVals.foreach(v => pw.print(v + ", "))
        pw.print(targetOnDay.adjClose + ", ")
        val synthVal = rowVals.zipWithIndex.map(f => summary.betas(f._2) * f._1).sum
        pw.print(synthVal + ", ")
        pw.println(synthVal - targetOnDay.adjClose)
      }
    )
    pw.close()
  }

}

