package datacloud.scala.tpfonctionnel

object MySorting {

  def isSorted[A](array: Array[A], f: (A, A) => Boolean): Boolean = {
    if (array.length == 0 || array.length == 1 || array.isEmpty) true
    else if (!f(array(0), array(1))) false
    else isSorted(array.tail, f)
  }

  def ascending[T](implicit ord: Ordering[T]) = (a: T, b: T) => ord.compare(a, b) <= 0

  def descending[T](implicit ord: Ordering[T]) = (a: T, b: T) => ord.compare(a, b) >= 0
}