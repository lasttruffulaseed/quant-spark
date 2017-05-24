
import per.harenp.Hedgehog._
import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object ScalaTests
{
  println("Running")
  val testString = "uri://base/subdir/keep-dontkeep.txt"
  val endString = testString.split("/").last
  val symbol = endString.split(Array('-','.')).head

  // ----------

    val conf = new SparkConf().
      setSparkHome("/Users/harenp/development/spark").
      setAppName("Market Data App").
      setMaster("local[2]")  // try 2 cores for now

    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId","AKIAJLPZARP4NKH4IJGA")
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey","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")

    /* -------------------------------------------------------------------
     * prepare S&P 500 equity RDDs
     */
    val dbBase = "s3n://hedgehog-markets/"
    val minsDir = dbBase + "raw-minute-data"
    val daysDir = dbBase + "raw-daily-data"
    val symbolPartitioner = new SymbolPartitioner

    // load symbols making up the index from s3 file
    val spxLines = sc.wholeTextFiles(dbBase + "spx-symbols.txt").map(pr=>pr._2).first().split("/n")

    // debug
    spxLines.foreach(s=>println("symb: " + s))

    // contents of each yahoo day-close historical data within the index,
    // partitioned to keep contents for same symbol together
    val symDailies = sc.wholeTextFiles(daysDir).partitionBy(symbolPartitioner).persist()
    //val s = symDailies.mapPartitions()

    // contents of each google 60-second result
    // also partitioned by symbol to avoid network shuffling
    val symMinutes = sc.wholeTextFiles(minsDir).partitionBy(symbolPartitioner).persist()


  var i : Int = 10  // an integer
  var j = 11        // also an integer
  val k = "foo"     // a read-only string value

  def az(s : String) : String = "yall " + s
  def mn(s : String) = {
    s + ", you betcha"
  }

/*  (x: Int) => x * 3   // triple function
  x => x + 3          // triple , but type is inferred
  _ * 3               // triple, shorthand (_ is argument)

    x => {              // just a bigger block of code
    val three = 3
    x * three
  }
*/

  def triple(x : Int): Int = 3 * x

  val myList = List(2, 4, 8)
  myList.foreach(x => println(x)) // prints 2, 4, 6
  myList.foreach(println)         // ditto
  myList.map(x => x * 3)          // returns a new List(6, 12, 24)
  myList.map(_ * 3)               // ditto
  myList.map(triple)              // ditto

  myList.filter(x => x>2)         // new List(4, 8)
  myList.filter(_>2)              // ditto

  myList.reduce((x, y) => x + y)  // 12
  myList.reduce(_ + _)            // ditto
  myList.sum                      // ditto

}

