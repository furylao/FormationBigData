object ObjectOriented {


  def main(args: Array[String]): Unit = {

    val facture1 = SchemaFacture("BK34", "20/10/2021", "dupont", 3500)
    println(facture1.factureid)
    println(facture1.nomClient)
    println(ScalaTraining.exctractFrist(facture1.nomClient, 2))

    val command1 = DetailFacture(facture1, "ki897c", 2, 800)
    val command2 = DetailFacture(SchemaFacture("BK35", "20/10/2021", "martin", 3500), "ki897c", 2, 800)

    command2.idFacture.factureid

    val commande3 = Commande(facture1, command1)
    val revenueFacture1 = commande3.revenuCompute(commande3.produitCommande.quantite, commande3.produitCommande.prixUnitaire)
    println(revenueFacture1)
  }
}
