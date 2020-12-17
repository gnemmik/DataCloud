package datacloud.scala.tpfonctionnel.catalogue

import datacloud.scala.tpobject.catalogue.CatalogueWithNonMutable

class CatalogueSoldeWithAnoFunction extends CatalogueWithNonMutable with CatalogueSolde {
  def solde(a: Int) = {
    map = map.mapValues(v => (v * a) / 100)
  }
}