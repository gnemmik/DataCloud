package datacloud.scala.tpfonctionnel

object Counters {
  def nbLetters(list: List[String]): Int = {
    return list.flatMap(_.replaceAll("\\s", "")).map(_ => 1).reduce(_ + _)
  }
}