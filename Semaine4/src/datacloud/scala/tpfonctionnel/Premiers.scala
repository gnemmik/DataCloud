package datacloud.scala.tpfonctionnel

object Premiers {
  
  def premiers(n: Int): List[Int] = {
    var list = (2 to 100).toList
    (2 to n).toList.foreach(i => list = list.filter(e => e <= i || e % i != 0))
    return list
  }

  def premiersWithRec(n: Int): List[Int] = {
    def f(list: List[Int]): List[Int] = {
      if (list(0) * list(0) > list(list.size - 1)) return list
      else return list(0) +: f(list.filter(e => e != list(0) && e % list(0) != 0))
    }
    var list = (2 to 100).toList
    return f(list)
  }
}