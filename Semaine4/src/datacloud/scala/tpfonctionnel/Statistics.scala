package datacloud.scala.tpfonctionnel

object Statistics {

  def average(list: List[(Double, Int)]): Double = {
    val res = list.map { case (n, c) => (n * c, c) }.reduce((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2))
    return res._1 / res._2
  }
}