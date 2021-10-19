case class Commande(

                   facture : SchemaFacture,
                   produitCommande : DetailFacture

                   )
{
  def revenuCompute(quantity : Int, prixUnitaire :Double): Double ={
    return quantity * prixUnitaire
  }

  def taxeCompute(tauxTva : Int, totalFacture :Double): Double ={
    return  Math.round(tauxTva * totalFacture)
  }
}
