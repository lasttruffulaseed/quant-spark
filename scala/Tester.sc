import scala.io.Source
import java.time._
import java.io._
import per.harenp.Hedgehog._
object Tester
{
  println("Welcome to the Scala worksheet")
  val dbBase = "/Users/harenp/localed/ub-ms/cs597-project/dev-base/project/"
  val minsDir = dbBase + "raw-minute-data"
  val daysDir = dbBase + "daily-historical"
  val u = Ute.nyTimeZoneId
  val spxLines = Source.fromFile(dbBase+"spx-symbols.txt").getLines()
  val spxEquities = spxLines.map(symbol => PriceDataMunging.equity(symbol, minsDir, daysDir))
  /*
  val aa = PriceDataMunging.equity("AA", minsDir, daysDir)
  val itot = PriceDataMunging.equity("AA", minsDir, daysDir)
  println("Daily volume = " + aa.yrAvgDailyVolume)
  aa.yrOfDaily(0).nyTradingDate
  itot.yrOfDailyClose(0)
  itot.yrOfDailyClose(241)
  */
  spxEquities.length



  println("foo")
}