package datacloud.scala.tpfonctionnel.catalogue

import datacloud.scala.tpobject.catalogue.CatalogueWithNonMutable

class CatalogueSoldeWithFor extends CatalogueWithNonMutable with CatalogueSolde {
  def solde(a: Int) = {
    for ((k, v) <- map) {
      storeProduct(k, (v * a) / 100)
    }
  }
}