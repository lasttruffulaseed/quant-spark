package per.harenp.Experimental

import breeze.plot._
import per.harenp.Hedgehog.PriceData

object Plotter {
  println("tester")
  def doPlot(series: Array[PriceData]): Figure =
  {
    val min = 0
    /* series.minBy(_.low)*/
    val max = series.length
    
    val rng = Range.Double(min, max, 1).toArray
    val prices = series.map(_.adjClose).toArray
  
    val f = Figure()
    val p = f.subplot(0)
    p += plot(rng, prices)
    p.xlabel = "Period"
    p.ylabel = "Price ($)"
    f
  }
}