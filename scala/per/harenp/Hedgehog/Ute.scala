package per.harenp.Hedgehog

import java.io.File
import java.time._

object Ute
{
  val nyTimeZoneId    = ZoneId.of("America/New_York")
  val nyTimeZoneRules = nyTimeZoneId.getRules
  def epochDay(epochSecs : Long) : Long = epochSecs/86400    // 86400 seconds per day
 }
