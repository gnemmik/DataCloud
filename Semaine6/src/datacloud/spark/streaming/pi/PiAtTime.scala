package datacloud.spark.streaming.pi

import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds

object PiAtTime extends App{
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  val conf = new SparkConf().setAppName("Pi At Time").setMaster("local[2]")
  val ssc = new StreamingContext(new SparkContext(conf), Seconds(1))
  
  val lines = ssc.socketTextStream("localhost", 4242)
  val split = lines.map(_.split(" "))
  val doubles = split.map{word => if (word(0).toDouble*word(0).toDouble+word(1).toDouble*word(1).toDouble < 1 ) 1 else 0 }
  val res = doubles.reduce(_+_).map(v => (3.14, 4.0 * v)).join(lines.count().map(v => (3.14, v))).map(x => x._2._1/x._2._2)
  
  res.print()
  
  ssc.start()
  println("*** Pi At Time ***")
  ssc.awaitTermination()
}