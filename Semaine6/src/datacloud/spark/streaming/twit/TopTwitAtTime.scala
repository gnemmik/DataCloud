package datacloud.spark.streaming.twit

import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds

object TopTwitAtTime extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  val conf = new SparkConf().setAppName("Top Twit At Time").setMaster("local[4]")
  val ssc = new StreamingContext(new SparkContext(conf), Seconds(1))

  val lines = ssc.socketTextStream("localhost", 4242)
  val split = lines.flatMap(_.split(" "))
  val filter = split.filter(word => word.contains('#'))
  val pairs = filter.map(word => (word, 1))
  val red = pairs.reduceByKey(_ + _)
  val res = red.transform(rdd => rdd.sortBy(_._2, false)).map(word => word._1)

  res.print()

  ssc.start()
  println("*** Top Twit At Time ***")
  ssc.awaitTermination()
}