package datacloud.scala.tpobject.catalogue

import scala.collection.immutable.Map
import scala.collection.mutable.ArrayBuffer

class CatalogueWithNonMutable extends Catalogue {
  private var map: Map[String, Double] = Map()

  def getPrice(name: String): Double = {
    for ((k, v) <- map) {
      if (k == name) return v
    }
    return -1.0
  }

  def removeProduct(name: String) = {
    for ((k, v) <- map) {
      if (k == name) {
        map = map - k
      }
    }
  }

  def selectProducts(min: Double, max: Double): Iterable[String] = {
    var it: ArrayBuffer[String] = ArrayBuffer()

    for ((k, v) <- map) {
      if (v >= min && v <= max) it += k
    }
    return it
  }

  def storeProduct(name: String, price: Double) = {
    map = map + (name -> price)
  }
}