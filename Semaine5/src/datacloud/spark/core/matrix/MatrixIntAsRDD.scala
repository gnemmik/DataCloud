package datacloud.spark.core.matrix

import org.apache.spark.rdd.RDD
import datacloud.scala.tpobject.vector.VectorInt
import org.apache.spark.SparkContext
import org.apache.log4j._
import shapeless.ops.tuple.ToList
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf

object MatrixIntAsRDD {
  implicit def RDDToMaxtrixIntAsRDD(vector: RDD[VectorInt]): MatrixIntAsRDD = return new MatrixIntAsRDD(vector)

  def makeFromFile(url: String, nbPartitions: Int, spark: SparkContext): MatrixIntAsRDD = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    return new MatrixIntAsRDD(spark.textFile(url, nbPartitions).map(_.split(" ")).map(x => x.map(_.toInt)).map(new VectorInt(_)).zipWithIndex().sortBy(_._2, true).map(_._1))
  }
}

class MatrixIntAsRDD(vector: RDD[VectorInt]) {
  val lines: RDD[VectorInt] = vector

  def nbLines(): Int = lines.count().toInt

  def nbColumns(): Int = lines.first().length()

  def get(i: Int, j: Int): Int = {
    if (! (0 <= i & i < nbLines()) & (0 <= j & j < nbColumns())) {
      return -1
    }
    return lines.zipWithIndex().filter(x => x._2 == i).map(_._1).first().get(j)
  }

  override def equals(a: Any): Boolean = {
    a match {
      case other: MatrixIntAsRDD => {
        if (other.nbLines() != nbLines()) return false
        if (other.nbColumns() != nbColumns()) return false
        val res = lines.zipWithIndex().map(x => (x._2, x._1)).join(other.lines.zipWithIndex().map(x => (x._2, x._1))).filter(x => !x._2._1.equals(x._2._2))
        return res.count() == lines.count()
      }
      case _ => false
    }
  }

  def +(other: MatrixIntAsRDD): MatrixIntAsRDD = {
    return lines.zipWithIndex().map(x => (x._2, x._1)).join(other.lines.zipWithIndex().map(x => (x._2, x._1))).sortByKey(false).map(x => x._2._1.+(x._2._2))
  }

  def transpose(): MatrixIntAsRDD = {
    val res = lines.zipWithIndex().map(v => (v._1.elements.zipWithIndex, v._2)).flatMap{case (x,y) => { x.map(x => (x._2,( y, x._1)))}}.groupByKey()
    return res.map{case (x,y) => {(x, new VectorInt(y.map(_._2).toArray))}}.sortByKey(false).map(x => x._2)
  }

  def *(other: MatrixIntAsRDD): MatrixIntAsRDD = {
    val matA = transpose()
    return matA.lines.zipWithIndex().map(x => (x._2, x._1)).join(other.lines.zipWithIndex().map(x => (x._2, x._1))).map(x => x._2._1.prodD(x._2._2)).map(x => x.reduce((x,y) => x.+(y)))
  }

  override def toString = {
    val sb = new StringBuilder()
    lines.collect().foreach(line => sb.append(line + "\n"))
    sb.toString()
  }
}

