package datacloud.scala.tpobject.catalogue

import scala.collection.mutable.Map
import scala.collection.mutable.Iterable
import scala.collection.mutable.ArrayBuffer

class CatalogueWithMutable extends Catalogue {
  private var map = Map[String, Double]()

  def getPrice(name: String): Double = {
    for ((k, v) <- map) {
      if (k == name) return v
    }
    return -1.0
  }

  def removeProduct(name: String) = {
    for ((k, v) <- map) {
      if (k == name) map -= k
    }
  }

  def selectProducts(min: Double, max: Double): Iterable[String] = {
    val it = ArrayBuffer[String]()

    for ((k, v) <- map) {
      if (v >= min && v <= max) it += k
    }
    return it
  }

  def storeProduct(name: String, price: Double) = {
    map += (name -> price)
  }
}