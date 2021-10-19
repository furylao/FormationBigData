case class CommandeVehicule() extends ModelCommande() {


  override def taxeCommande(): Double = super.taxeCommande()

  override def revenueCommande(): Double = super.revenueCommande()


}
