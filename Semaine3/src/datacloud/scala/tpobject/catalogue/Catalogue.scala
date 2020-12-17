package datacloud.scala.tpobject.catalogue

trait Catalogue {
  def getPrice(name: String): Double
  def removeProduct(name: String)
  def selectProducts(min: Double, max: Double): Iterable[String]
  def storeProduct(name: String, price: Double)
}