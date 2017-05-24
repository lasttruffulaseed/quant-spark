package per.harenp.Hedgehog

import java.time._
import scala.io.Source
import java.io.File
import org.apache.spark.SparkContext

/**
 * @author patrick.haren@yahoo.com
 */
object PriceDataMunging {
  
//  val nyTimeZoneId    = ZoneId.of("America/New_York")
//  val nyTimeZoneRules = nyTimeZoneId.getRules()
//  def epochDay(epochSecs : Long) : Long = epochSecs/86400    // 86400 seconds per day
 
  /** --------------------------------------------------------------------------------
   * Converts a google format 1 min time series to array of PriceData
   * epochTime is in seconds since epoch
   * now need to pass daily data, to leverage the adjuster for each day
   */
  def toPerMinStockSeries(fileLines: Iterator[String], dailyData: Array[PriceData]) : Array[PriceData] =
  {
    /* google format:
     *  DATE,CLOSE,HIGH,LOW,OPEN,VOLUME
     *  a1423751460,39.60,39.6,39.53,39.58,12922
     *  or        1,39.80,39.8,39.60,39.60,11499
     */
    var epochOffset = 0   // need state to accompany the interpretation of each line :(
    val adjusterPerDay = dailyData.map(point => (Ute.epochDay(point.epochTime), point.adjuster)).toMap
    
    /* Provide (base-time, offset), where base-time is seconds since epoch and offset is seconds since base-time,
     * given a line that could start with a new base (prefixed with 'a') or a new offset (in minutes)
     */
    def interpretedEpochTime(currentBase: Int, timeField: String) : (Int, Int) =
    {
      /* google timestamp is either an 'a' followed by time (in seconds since epoch), or an offset number of 60 seconds */
      if (timeField.startsWith("a")) (timeField.tail.toInt, 0) else (currentBase, 60*timeField.toInt)
    }

    /* PriceData from an input line
     */
    def dataFromLine(line: String) : Option[PriceData] = line.split(",") match
    {
      case Array("COLUMNS=DATE",_*) => None
      case Array(dateField, closeField, highField, lowField, openField, volumeField) =>
      {
        try {
          val (base, offset) = interpretedEpochTime(epochOffset, dateField)
          val adjuster = adjusterPerDay(Ute.epochDay(base))
          epochOffset = base
          Some(PriceData(base + offset, openField.toDouble, closeField.toDouble, highField.toDouble, lowField.toDouble, volumeField.toInt, adjuster))
        }
        catch
        {
          case _ : Throwable =>
            println("TODO: houston...")
            None
        }
      }
      case anythingElse => None
    }
  
    fileLines.flatMap(dataFromLine).toArray.distinct
  }
  
  /** --------------------------------------------------------------------------------
   * Reads a history in the Yahoo format
   */
  def toPerDayStockSeries(fileLines: Iterator[String]) : Array[PriceData] =
  {
    /* yahoo format:
     *  Date,Open,High,Low,Close,Volume,Adj Close
     *  2015-03-20,42.39,42.53,42.20,42.21,2216600,42.21
     */

    // def isAllDigits(x: String) = x forall Character.isDigit

    /* PriceData from an input line
     */
    def dataFromLine(line: String) : PriceData = line.split(",") match
    {
      case Array(dateField, openField, highField, lowField, closeField, volumeField, adjustedCloseField) =>
      {
        val date = LocalDate.parse(dateField)
        val zonedDate = ZonedDateTime.of(date, LocalTime.of(9,30), Ute.nyTimeZoneId)
        val epochDate = zonedDate.toInstant.getEpochSecond
        val close = closeField.toDouble
        val adjuster = adjustedCloseField.toDouble/close
        PriceData(epochDate, openField.toDouble, close, highField.toDouble, lowField.toDouble, volumeField.toLong, adjuster)
      }
      case whatever =>
      {
        PriceData(0,0,0,0,0,0,0)    // this is just for breakpoint. condition should not happen on a good data file
      }
    }

    fileLines.next()  // skip the header
    fileLines.map(dataFromLine).toArray.reverse
  }


  /** --------------------------------------------------------------------------------
   * Pads sequence with any missing time periods 
   */
  def filledWithMissingTradeMins(rawSeries : Seq[PriceData], tradeDays : Array[LocalDate]) =
  {
    /* approach:
       ---------
       set lastClose var to something reasonable for initial value (first open in series)
       for each tradeDay {
         define start and end second vals from epoch (start-sec, end-sec)
         seriesForDay = series with span of this tradeDay
         currentPrice var = lastClose
         for each series-point {
          fill from current minute to one less than series-point minute using current price
          emit series-point
          currentPrice = series-point price
          current minute = series-point minute
          } (/series point)
         fill from current minute to end minute using current price
         set lastClose to current price
       } (/tradeday)
    */

    def filledPoints(startSecs : Long, endSecs : Long, price : Double, adjuster : Double) =
    {
      (startSecs until endSecs by 60).map(sec => PriceData(sec, price, price, price, price, 0, adjuster))
    }
    
    var currentPrice = rawSeries.head.close
    var currentAdj = rawSeries.head.adjuster
    tradeDays.flatMap { tradeDate =>
      val tradeTime = tradeDate.atTime(9, 30, 0)
      val dayOffset = Ute.nyTimeZoneRules.getOffset(tradeTime)
      val startSec = tradeTime.toEpochSecond(dayOffset)  
      val endSec = startSec + 390*60 +1   /* 390 minutes between 9:30 and 16:00 */
      val daysSeries = rawSeries.filter(_.nyTradingDate.isEqual(tradeDate))
      
      var currentSec = startSec
      val fill = daysSeries.flatMap { dataPoint =>
        /*dataPoint*/
        val fromSec = currentSec
        val toSec = dataPoint.epochTime
        currentPrice = dataPoint.close
        currentAdj = dataPoint.adjuster
        currentSec = toSec + 60
        filledPoints(fromSec, toSec, currentPrice, currentAdj) :+ dataPoint
       }
      fill ++ filledPoints(currentSec, endSec, currentPrice, currentAdj)
    }
  }

  def smoothedEquity(firstTradeDate: LocalDate, name: String, dailyPrices : Array[PriceData], minutePrices : Array[PriceData]) =
  {
    // use yahoo daily data to identify the dates that should have by-minute data (tradeDays) on and after first date
    val tooEarly = firstTradeDate.minusDays(1)
    val tradeDays = dailyPrices.filter(p => tooEarly.isBefore(p.nyTradingDate)).map(_.nyTradingDate)
    val smoothedMinuteData = filledWithMissingTradeMins(minutePrices,tradeDays)
    EquityData(name, dailyPrices, smoothedMinuteData)
  }

  /** --------------------------------------------------------------------------------
   * Builds a full equity info object
   * could make the paths into broadcast variables, or a shared config?
   */
  def equityOnLocalFS(firstTradeDate: LocalDate, name: String, rawMinBasePath: String, rawDailyBasePath : String) : EquityData = {

    val dailyFile = Source.fromFile(rawDailyBasePath + "/" + name + ".txt")
    val dailyData = toPerDayStockSeries(dailyFile.getLines())

    // get list of files in the directory, who's name starts with base
    def dataFiles(dirName: String, base: String) : List[File] = {
      val dir = new File(dirName)
      dir.listFiles.filter(_.getName.startsWith(base)).toList
    }

    def workaround(lines: Iterator[String]) : String = lines.mkString("\n")

    val rawMinuteData = dataFiles(rawMinBasePath, name + "-").map(f => workaround(Source.fromFile(f).getLines) ).reduce(_+"\n"+_) ///*.mkString(" ")*/)//.reduce(_+_)

    val minuteData = toPerMinStockSeries(rawMinuteData.lines, dailyData) //.toArray

    //val minuteData = dataFiles(rawMinBasePath, name + "-").flatMap(f => toPerMinStockSeries(Source.fromFile(f).getLines(), dailyData)).toArray

    print(name + " ")

    smoothedEquity(firstTradeDate, name, dailyData, minuteData)
  }

  /** --------------------------------------------------------------------------------
   * Builds a collection of equities info map of equity objects
   * TODO: define this, as split work to load eq using 2 RDDs, where this function uses the elems within RDD...
   * Could read both data-sets and use common keys to keep together in partitions...
   * (a bit more definition on client for my taste)...
   */
  def equitiesOnSpark(firstTradeDate: LocalDate, rawMinBasePath: String, rawDailyBasePath : String, sc: SparkContext) /* : RDD[(String, EquityData)]*/ = {

    val symbolPartitioner = new SymbolPartitioner
    def fileNamesToSymbols(it: Iterator[(String, String)]) = it.map(p=>(symbolPartitioner.actualSymbol(p._1), p._2))
    def buildEquity(name: String, packet: (String, Array[PriceData])) = {
      val minuteData = toPerMinStockSeries(packet._1.lines, packet._2)
      smoothedEquity(firstTradeDate, name, packet._2, minuteData)
    }

    // RDD for contents of each yahoo file of historical day-close stock data
    // partitioned to keep contents for same symbol together.
    // wholeTextFiles provides symbol name within key (which is file/path name).
    val dayFiles = sc.wholeTextFiles(rawDailyBasePath).partitionBy(symbolPartitioner).persist()
    val symbolDailies = dayFiles.mapPartitions(fileNamesToSymbols,true)   // content of each daily file, keyed by symbol
    val symbolDayData = symbolDailies.mapValues(contents => toPerDayStockSeries(contents.lines)).persist()

    // RDD for contents of each google 60-second historical data within the index,
    // partitioned to keep contents for same symbol together.
    // wholeTextFiles retains multiple period files per symbol (need to conform keys first)
    val minuteFiles = sc.wholeTextFiles(rawMinBasePath).partitionBy(symbolPartitioner).persist()
    val symbolMinutePieces = minuteFiles.mapPartitions(fileNamesToSymbols,true)
    val symbolMinutes = symbolMinutePieces.reduceByKey(_ + _)   // content of each per min, keyed by symbol

    // This should be an inexpensive join, since spark will not need to shuffle
    val symbolPackets = symbolMinutes.join(symbolDayData)   // key, (minuteText, dayData)

    // RDD to return, should be an RDD with value = equity case instance (of minute data and day-data)
    //symbolPackets.mapPartitions(_.map(kp => (kp._1, buildEquity(kp._1, kp._2))),true)
    symbolPackets.mapPartitions(_.map(kp => buildEquity(kp._1, kp._2)),true)
  }
}