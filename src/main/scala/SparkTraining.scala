import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.expressions.NamePlaceholder.nullable
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types._

import java.io._
import java.util.Properties

object SparkTraining {

  val schemaOrderline = StructType(
    List(
      StructField("OrderLine", IntegerType, false),
      StructField("Orderid", IntegerType, false),
      StructField("Productid", IntegerType, false),
      StructField("Shipdate", DateType, false),
      StructField("Billdate", DateType, false),
      StructField("Unitprice", DoubleType, false),
      StructField("Numunits", IntegerType, false),
      StructField("Totalprice", DoubleType, false)
    )
  )

  var orderSchema = StructType(
    List(
      StructField("Orderid", IntegerType, nullable),
      StructField("CustomerId", IntegerType, nullable),
      StructField("CampaignId", IntegerType, nullable),
      StructField("OrderDate", DateType, nullable),
      StructField("City", StringType, nullable),
      StructField("State", StringType, nullable),
      StructField("ZipCode", StringType, nullable),
      StructField("PaymentType", StringType, nullable),
      StructField("TotalPrice", DoubleType, nullable),
      StructField("NumOrderLines", IntegerType, nullable),
      StructField("NumUnits", IntegerType, nullable)
    )
  )


  def main(args: Array[String]): Unit = {

    val hiveDir = new File("chemin de warehouse")
    val f = new File("/D:system/hive")

    val sparkSession = SparkSession.builder()
      .appName("Mon application")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", f.getAbsolutePath)
      //.enableHiveSupport()
      .config("spark.sql.crossjoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.shuffle.compress", "true")
      .getOrCreate()


    val sparkContext = sparkSession.sparkContext.parallelize(Seq("tom", "vince", "tim", "paul", "pierre"))
    sparkContext.foreach(e => println(e))

    val rdd2 = sparkSession.sparkContext.parallelize(List(12, 23, 36, 58, 98))
    rdd2.map(e => e * 2).foreach(e => println(e))

    val rdd3 = sparkSession.sparkContext
      .textFile("C:\\Users\\Administrateur\\Desktop\\data")
      .flatMap(e => e.split(" "))

    // rdd3.foreach(i => println(i))

    //import pour l'accès à l'objet dataFrame
    import sparkSession.implicits._

    //création du dataframe df à partir du rdd(Resilient distributed dataset)
    val df1 = rdd3.toDF()

    //print les 3 premier élement du dataframe df1
    // df1.show(3)

    val dforderline = sparkSession
      .read
      .format("com.databricks.spark.csv")
      .schema(schemaOrderline)
      .option("header", "true")
      .option("delimiter", "\t")
      .option("inferSchema", "false")
      .load("C:\\Users\\Administrateur\\Desktop\\data\\orderLine.txt")

    val dforders = sparkSession
      .read
      .format("com.databricks.spark.csv")
      .schema(orderSchema)
      .option("header", "true")
      .option("delimiter", "\t")
      .option("inferSchema", "false")
      .load("C:\\Users\\Administrateur\\Desktop\\data\\orders.txt")


    //dforderline.show(15)
    //permet de  voir la structure/schéma des données
    dforderline.printSchema()

    // selection des colonnes
    //dforderline.select("OrderLine")


    val dforc = sparkSession
      .read
      .format("orc")
      .option("compression", "snappy")
      .load("C:\\Users\\Administrateur\\Desktop\\data\\part-r-00000-2c4f7d96-e703-4de3-af1b-1441d172c80f.snappy.orc")

    dforc.show(5)

    val df_prix2 = dforderline
    //    .select(
    //      $"UnitPrice".as("Unit Price"),
    //      $"numunits",
    //      $"totalprice",
    //      $"OrderLine"
    //    )

    //    val df_prix3 = dforderline.selectExpr(listeCols:_*)
    //    df_prix3.printSchema()

    //ajouter une colonne calculée
    /* exo : créez une colonne "promo" qui :
       totalprice < 200 alors 0
     totalprice entre 200 et 600 alors 0,05
     totalprice > 600 alors 0,07
     ajouter "promoAmount" = promo * totalprice
     ajouter "totalBill = totalprice - promoAmount - taxes*/
    val df_ordersTaxes = dforderline
    //      .withColumn("taxes", col("numunits") * col("totalprice")* lit(20))
    //      .withColumn("numunits", col("numunits").cast(StringType))
    //      .withColumn("shipdate", col("shipdate").cast(DateType))
    //      .withColumn("promo", when(col("totalprice")<200, 0))
    //      .withColumn("promoAmount", col("numunits") * col("totalprice"))
    //      .withColumn("totalbill", col("totalprice") - col("promoAmount")- col("taxes"))
    //        .otherwise(when(col("totalprice")<200, 0))
    //        .otherwise(when(col("totalprice")>600, 0.07))
    //        .otherwise(when(col("totalprice")>200 && col("totalprice")<600, 0.05)))

    df_ordersTaxes.show(10)

    //filtre
    val df_orderfiltre = df_ordersTaxes
      .filter(col("numunits") === lit(2))
      .filter(col("numunits") > lit(2) && col("taxes") == lit(0))

    df_orderfiltre.show(10)

    //jointure
    //    val df_join = dforderline.join(df_ordersTaxes, Seq("orderline"), "inner")
    //      .filter(col("numunits") > lit(2))

    //    val df_join2 = dforderline.join(df_ordersTaxes, Seq("orderline"), "inner")
    //      .join(df_ordersTaxes, col("orderlineId") === col("orderline"))
    //.join(df_ordersTaxes, df_ordersTaxes("orderlineId") === dforderline("orderline"))

    //Union de dataframes
    val df_union = dforderline.union(df_ordersTaxes.union(df_prix2))
    df_union.show(10)


    val dfjoined = dforders.join(dforderline, Seq("orderid"), "inner")
    dfjoined.show(10)


    //register dataframe in datastore/warehouse
    dforders.createOrReplaceTempView("table_commande")

    //query datawarehouse
    val df_hive = sparkSession.sql("select * from table_commande limit(100)")
    println(df_hive.count())
    df_hive.show(150)

    //union + calcul
   // val df_kpi = df_union.groupBy(col("state")).sum("totalprice")
    //
    //Connect to database
//    val df_mysql1 = sparkSession.read
//      .format("jdbc")
//      .option("url", "jdbc:mysql://127.0.0.1:3306?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC")
//      .option("user", "consultant")
//      .option("password", "pwd#86")
//      .option("query","")
//      .option("dbtable", "(select state, city, sum(round(numunits * totalprice)) as commandes_totales from jea_db.orders group by state, city) requete")
//      .load()

    val df_mysql2 = sparkSession.read
      .format("jdbc")
      .option("url", "jdbc:mysql://127.0.0.1:3306?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC")
      .option("user", "consultant")
      .option("password", "pwd#86")
      .option("dbtable", "(select state, city, sum(round(numunits * totalprice)) as commandes_totales from jea_db.orders group by state, city) requete")
      .load()

    df_mysql2.show()
   // df_mysql1.show()

    //Ecriture sur un fichier à partir d'un dataframe(Generation de plusieurs fichiers (._SUCCESS, .part, .csv,SUCCESS )
//    df_mysql2.write
//      .format("com.databricks.spark.csv")
//      .option("header","true")
//      .csv(*/"C:\\Formation\\tableMysql.csv")

    //Ecriture sur un fichier (ou plusieurs en fonction du nombre de processus spécifier à l'initialisation de la session spark avec l'option .master
    //
    //      .master("local[*]") équivalent au nombre de coeur du processeur
    //
    // ) à partir d'un dataframe(Generation de plusieurs fichiers (._SUCCESS, .part, .csv,SUCCESS )
   /* dforders.write
      .format("com.databricks.spark.csv")
      .option("header","true")
      .option("delimiter",";")
      .mode(SaveMode.Overwrite)
      .csv("C:\\Formation\\tableMysql")

    dforderline.write
      .format("parquet")
      .option("header","true")
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .save("C:\\Formation\\tableOrderLineParquet")

    //spécification du nombre de processus avec .repartition sur le dataframe qui permet d'utiliser l'absraction des blocs RDD
    // et ainsi optimiser les traitements au lieu du shuffle qui deplace les partitions de données vers le noeud de calcul
    //attention le nombre de repartition ne peut être supérieur au nombre de processus(processeurs)
    dforderline.repartition(1)
      .write
      .format("orc")
      .option("compression", "snappy")
      .option("header","true")
      .mode(SaveMode.Overwrite)
      .save("C:\\Formation\\tableOrderLineOrc")

    //rename output file and delete generated files from spark
    import org.apache.hadoop.fs._;

    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration);

    val file = fs.globStatus(new Path("C:\\Formation\\tableMysql\\part*"))(0).getPath().getName();

    fs.rename(new Path("C:/Formation/tableMysql/" + file), new Path("C:/Formation/tableMysql/mydata.csv"));

    fs.delete(new Path("C:/Formation/tableMysql/mydata.csv-temp"), true);

    //decouper les fichier par paramètre/champ
    dforders.repartition(1)
      .write
      .partitionBy("city")
      .format("orc")
      .option("compression", "snappy")
      .option("header","true")
      .mode(SaveMode.Overwrite)
      .save("C:\\Formation\\tableOrdersOrc")
*/
  }
}
