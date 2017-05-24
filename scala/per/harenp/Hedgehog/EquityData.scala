package per.harenp.Hedgehog


//import breeze.linalg._
//import breeze.math._
//import breeze.numerics._
import breeze.stats._


/**
 * @author patrick.haren@yahoo.com
 */
case class EquityData(name: String, dailyPrices : Array[PriceData], minutePrices : Array[PriceData])
{
  val tradingDaysPerYr = 252
  val totalDays = dailyPrices.length
  val totalMins = minutePrices.length

  val yrOfDaily = dailyPrices.slice(totalDays-tradingDaysPerYr , totalDays-1)
  val yrOfDailyClose = yrOfDaily.map(_.adjClose)
  val allMinClose = minutePrices.map(_.adjClose)
  
  def yrAvgDailyVolume = yrOfDaily.map(_.volume).sum/tradingDaysPerYr   // reduceLeft(_ + _)/tradingDaysPerYr
  def yrStdDev = stddev(yrOfDailyClose)
  def yrMean = mean(yrOfDailyClose)
}
