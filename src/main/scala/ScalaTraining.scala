import java.util.Properties


object ScalaTraining {

  def main(args: Array[String]): Unit = {

    val mavar: String = "Hello world"

    println(mavar)

    val mavar1: String = mavar + "test de la variable0";

    val test = 1

    val newText: String = zxtractFirst(mavar, 3)
    println(newText)

    var i = 0
    while (i < 10) {
      println(s"voici la valeur de  ${i}")
      i = i + 1
    }

    for (i <- 1 to 10) {
      println(s"voici la valeur de  ${i}")
      var j = i + 1
    }

    //Liste
    val maliste: List[String] = List("test", "zest", "lest")
    maliste.foreach(e => e.substring(2, 3))
    maliste.foreach { e => println(s"élement de la liste : ${e}") }


    val maliste2 = maliste.map(m => m.substring(0, 2))
    val maliste3 = maliste2.filter(f => f == "t")
    maliste3.foreach { e => println(s"filtre : ${e}") }

    maliste2.foreach {
      e => println(e)
    }

    val intList: List[Int] = List(10, 48, 89, 100, 46)
    val resltatList = intList.map(e => e * 3)
    val resltatList2 = intList.map(_ * 3)
    resltatList.foreach { e => println(e) }

    // tuple
    val tuple1: (String, Int, Boolean, String) = ("test", 10, true, "brest")


    //map
    val map1: Map[String, String] = Map("cle1" -> "valeur1", "cle2" -> "valeur2", "cle3" -> "valeur3")
    val map2: Map[String, Int] = Map("dist1" -> 100, "dist2" -> 200, "dist3" -> 300)
    val map3: Map[String, List[String]] = Map("villes" -> List("paris", "angers"), "pays" -> List("Japon", "France"), "continent" -> List("afrique", "amérique"))

    map1.keys.foreach(k => println((s"clé de ma map : ${k}")))
    val t = map1("cle1")
    println(t)

    map3.values.foreach { l => {
      l.foreach { e => println(s"valeur de ma map 3 ${e}") }
    }
    }

    //Array tableau
    val monTableau: Array[String] = Array("moi", "juvénal", "bruno", "vincent")
    monTableau(0)
    val liste = monTableau.toList

    //Séquence
    val maseq :Seq[String] = Seq("moi", "juvénal", "bruno", "vincent")

  }


  def exctractFrist(texte: String, longueur: Int): Unit = {
    val resultat = texte.substring(longueur)
    println("Votre texte extrait est : " + resultat)
  }

  def zxtractFirst(texte: String, longueur: Int): String = {
    val resultat = texte.substring(longueur)
    println("Votre texte extrait est : " + resultat)
    return resultat
  }


}
