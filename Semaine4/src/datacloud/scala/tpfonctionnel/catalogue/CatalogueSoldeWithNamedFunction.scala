package datacloud.scala.tpfonctionnel.catalogue

import datacloud.scala.tpobject.catalogue.CatalogueWithNonMutable

class CatalogueSoldeWithNamedFunction extends CatalogueWithNonMutable with CatalogueSolde {

  def solde(pourcent: Int) = {
    map = map.mapValues(diminution(_, pourcent))
  }

  def diminution(a: Double, pourcent: Int): Double = a * ((100.0 - pourcent) / 100.0)

}