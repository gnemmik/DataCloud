package datacloud.scala.tpobject.vector

class VectorInt(tab: Array[Int]) {
  val elements: Array[Int] = tab

  def length(): Int = {
    return elements.length
  }

  def get(i: Int): Int = {
    return elements(i)
  }

  def tostring(): String = {
    val str = StringBuilder.newBuilder
    for (e <- elements) {
      str.append(e)
    }
    return str.toString()
  }

  override def equals(a: Any): Boolean = {
    a match {
      case a: VectorInt => {
        if (!a.isInstanceOf[VectorInt]) return false
        if (length() != a.length()) return false

        for (i <- 0 to length() - 1) {
          if (get(i) != a.get(i)) return false
        }
        return true
      }
      case _ => false
    }
  }

  def +(other: VectorInt): VectorInt = {
    val tmp: Array[Int] = new Array(length())
    for (i <- 0 to length() - 1) {
      tmp.update(i, get(i) + other.get(i))
    }
    return new VectorInt(tmp)
  }

  def *(v: Int): VectorInt = {
    val tmp: Array[Int] = new Array(length())
    for (i <- 0 to length() - 1) {
      tmp.update(i, get(i) * v)
    }
    return new VectorInt(tmp)
  }

  def prodD(other: VectorInt): Array[VectorInt] = {
    val array: Array[VectorInt] = new Array(length())
    for (i <- 0 to length() - 1) {
      array.update(i, other.*(get(i)))
    }
    return array
  }
}

object VectorInt {
  implicit def IntToVectorInt(array: Array[Int]): VectorInt = {
    return new VectorInt(array)
  }
}
