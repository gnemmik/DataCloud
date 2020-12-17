package datacloud.spark.streaming.twit

import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds

object TowardsTopTwit extends App{
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  val conf = new SparkConf().setAppName("Towards Top Twit").setMaster("local[4]")
  val ssc = new StreamingContext(new SparkContext(conf), Seconds(1))
  
  val lines = ssc.socketTextStream("localhost", 4242)
  val split = lines.flatMap(_.split(" "))
  val filter = split.filter(word => word.contains('#'))
  val pairs = filter.map(word => (word, 1))
  
  val res = pairs.updateStateByKey((vals, state: Option[Int]) => state match {
    case None => if(vals.length == 0) Some(0) else Some(vals.reduce(_+_))
    case Some(n) => if(vals.length == 0) Some(n) else Some(n+vals.reduce(_+_))
  }).transform(rdd => rdd.sortBy(_._2, false)).map(_._1) 
  
  println("*** Towards Top Twit ***")
  res.print()
  
  ssc.checkpoint(".")
  ssc.start()
  ssc.awaitTermination()
}