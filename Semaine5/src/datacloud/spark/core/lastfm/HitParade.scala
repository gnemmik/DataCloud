package datacloud.spark.core.lastfm

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object HitParade {

  case class TrackId(id: String)

  case class UserId(id: String)

  def loadAndMergeDuplicates(context: SparkContext, url: String): RDD[((UserId, TrackId), (Int, Int, Int))] = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    return context.textFile(url).map(_.split(" ")).map(x => ((UserId(x(0)), TrackId(x(1))), (x(2).toInt, x(3).toInt, x(4).toInt))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
  }

  def hitparade(rdd: RDD[((UserId, TrackId), (Int, Int, Int))]): RDD[TrackId] = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    return rdd.map(x => (x._1._2, (if ((x._2._1 + x._2._2) > 0) 1 else 0, x._2._1 + x._2._2 - x._2._3))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).sortBy(x => ((-x._2._1, -x._2._2), x._1.id)).map(x => x._1)
  }
}