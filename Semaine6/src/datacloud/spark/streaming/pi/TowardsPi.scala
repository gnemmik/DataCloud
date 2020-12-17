package datacloud.spark.streaming.pi

import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds

object TowardsPi extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  val conf = new SparkConf().setAppName("Towards Pi").setMaster("local[4]")
  val ssc = new StreamingContext(new SparkContext(conf), Seconds(1))
  
  val lines = ssc.socketTextStream("localhost", 4242)
  val split = lines.map(_.split(" "))
  val doubles = split.map { word => if (word(0).toDouble * word(0).toDouble + word(1).toDouble * word(1).toDouble < 1) 1 else 0 }
  val joined = doubles.reduce(_ + _).map(v => (3.14, 4.0 * v)).join(lines.count().map(v => (3.14, v)))
  val cumul = joined.updateStateByKey((vals, state: Option[(Double, Long)]) => state match {
    case None => if (vals.length == 0) Some(0.0, 0L) else Some(vals.reduce((x, y) => (x._1 + y._1, x._2 + y._2)))
    case Some(n) => if (vals.length == 0) Some(n._1, n._2) else Some(vals.reduce((x, y) => (x._1 + y._1 + n._1, x._2 + y._2 + n._2)))
  })
  val res = cumul.map(x => x._2._1 / x._2._2)

  res.print()

  ssc.checkpoint(".")
  ssc.start()
  println("*** Towards Pi ***")
  ssc.awaitTermination()
}