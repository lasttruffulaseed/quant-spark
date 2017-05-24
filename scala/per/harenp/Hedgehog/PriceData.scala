package per.harenp.Hedgehog

import java.time.Instant
import java.time.ZoneId

/**
 * @author patrick.haren@yahoo.com
 */

/* US equity pricing data */
case class PriceData(epochTime : Long, open : Double, close : Double, high : Double, low: Double, volume: Long, adjuster: Double)
{
  def tradingTime(tz: String) = Instant.ofEpochSecond(epochTime).atZone(ZoneId.of(tz)).toLocalTime
  def tradingDate(tz: String) = Instant.ofEpochSecond(epochTime).atZone(ZoneId.of(tz)).toLocalDate
  def nyTradingDate = tradingDate("America/New_York")
  def nyTradingTime = tradingTime("America/New_York")
  def adjClose = close * adjuster
  /* .formatted("DD-MM-YYY : hh:mm") */
  /* LocalDateTime.ofEpochSecond(epochTime, 0, ZoneOffset.UTC).toLocalTime() */
  /* Instant.ofEpochSecond(epochTime) .formatted("") .atZone(ZoneId.of(tz)).formatted("DD-MM-YYY : hh:mm") */
}
