/**
 * @author patrick.haren@yahoo.com
 *
 */

import java.io.File
import java.time.LocalDate
import scala.io.Source
import per.harenp.Hedgehog._


/**
 * Price analysis using local file-system files
 * all directories are under the project...
 * args(0) is directory for minute-by-minute price files - "raw-minute-data" or "sample-mins"
 * args(1) is directory for daily-historical price files - "raw-daily-data" or "sample-days"
 * args(2) is basket of stock names to use (i.e. index) - "spx-symbols.txt" or "spx-sample.txt"
 */
object MarketsLocal extends App
{
  /* -------------------------------------------------------------------
   */

  val dbBase = "/Users/harenp/localed/ub-ms/cs597-project/dev-base/project/"
  val minsDir = dbBase + args(0)
  val daysDir = dbBase + args(1)
  val basketName = args(2)

  val firstDate = LocalDate.parse("2015-02-12")

  val fullList = new File(daysDir).list().map(f => f.split(".txt").head)       // all symbols that have data
  val basketList = Source.fromFile(dbBase + basketName).getLines().toArray    // symbols in the index/basket

  /* ----------------------------------------------------------
   * read the full set of equities (sample only) and produce a basket of equities for research
   */
  val equities = fullList.map(symbol => PriceDataMunging.equityOnLocalFS(firstDate, symbol, minsDir, daysDir)).toArray
  val basketEquities = equities.filter(equity => basketList.contains(equity.name))

  val len = basketEquities.length // stop being lazy!
  val fullLen = equities.length

  /* ----------------------------------------------------------
   * test cointegration with a single entity
   */
  //val targetEquity = equities.filter(e => "AET".equals(e.name)).head
  val targetEquity = equities.filter(e => "ITOT".equals(e.name)).head

  val betas = StockCoIntegration.cointegrated(targetEquity, basketEquities, 800, 80)

  val result = StockCoIntegration.fitOfFactorModel(targetEquity, basketEquities, 80, betas)

  print("unity,  \t")
  basketEquities.foreach(c => printf("%-11s", c.name + ",\t"))
  println
  betas.foreach(b => printf("% 4.6f,\t", b))
  println
  println("mean of residuals: " + result._1)
  println("variance of residuals: " + result._2)
  println("variance ratio at day 30: " + result._3)

  /* ----------------------------------------------------------
   * test with a smaller basket (just the top 5 contributors)
   */

  val research = StockCoIntegration.optimalSmallBasket(targetEquity, basketEquities, 800, 80, 5)

  research.contributors.foreach(c => printf("%-11s", c + ",\t"))
  println
  research.betas.foreach(b => printf("% 4.6f,\t", b))
  println
  println("mean of residuals: " + research.residualMean)
  println("variance of residuals: " + research.residualVariance)
  println("variance ratio at day 30: " + research.day30VR)
}
