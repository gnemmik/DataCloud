package datacloud.spark.core.stereoprix

import org.apache.spark._
import org.apache.log4j._
import shapeless.ops.nat.ToInt

object Stats {

  def chiffreAffaire(url: String, annee: Int): Int = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Chiffre Affaire Annee").setMaster("local[4]")
    val spark = new SparkContext(conf)
    val res = spark.textFile(url).filter(x => x.contains("_" + annee + "_")).map(x => x.split(" ")).map(x => (x(2).toInt)).reduce(_ + _)
    spark.stop()
    return res
  }

  def chiffreAffaireParCategorie(in: String, out: String) = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Chiffre Affaire Categorie").setMaster("local[4]")
    val spark = new SparkContext(conf)
    val res = spark.textFile(in).map(_.split(" ")).map(x => (x(4), x(2).toInt)).reduceByKey(_ + _).map(x => x._1 + ":" + x._2).saveAsTextFile(out)
    spark.stop()
  }

  def produitLePlusVenduParCategorie(in: String, out: String) = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Chiffre Affaire Categorie").setMaster("local[4]")
    val spark = new SparkContext(conf)
    val res = spark.textFile(in).map(_.split(" ")).map(x => ((x(4), x(3)), 1)).reduceByKey(_ + _).map(x => (x._1._1, (x._1._2, x._2))).reduceByKey((x, y) => if (x._2 > y._2) x else y).map(x => x._1 + ":" + x._2._1).saveAsTextFile(out)
    spark.stop()
  }
}