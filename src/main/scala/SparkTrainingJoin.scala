import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.NamePlaceholder.nullable
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types._


object SparkTrainingJoin {


  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("Mon application")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.sql.crossjoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.shuffle.compress", "true")
      .getOrCreate()

    //###### Exercice: Jointure de tableaux ######
    // Définition des schémas pour les fichiers à lire
    val schema_1 = StructType(
      Seq(
        StructField("orderid", IntegerType, nullable = false),
        StructField("customerid", IntegerType),
        StructField("campaignid", IntegerType),
        StructField("orderdate", DateType),
        StructField("city", StringType),
        StructField("state", StringType),
        StructField("zipcode", StringType),
        StructField("paymenttype", StringType),
        StructField("totalprice", DoubleType),
        StructField("numorderlines", IntegerType),
        StructField("numunits", IntegerType)
      )
    )
    val schema_2 = StructType(
      Seq(
        StructField("orderlineid", IntegerType),
        StructField("orderid", IntegerType, nullable = false),
        StructField("productid", IntegerType),
        StructField("shipdate", DateType),
        StructField("billdate", DateType),
        StructField("unitprice", DoubleType),
        StructField("numunits", IntegerType),
        StructField("totalprice", DoubleType)
      )
    )
    // Lecture des fichiers csv avec les schémas associés
    val df_orders_lines = sparkSession.read
      .format("com.databricks.spark.csv")
      .schema(schema_1)
      .option("header", "true")
      .option("sep", "\t")
      .option("inferSchema", "false")
      .load("C:\\Users\\Administrateur\\Desktop\\data\\orders.txt")
    val df_orders_2 = sparkSession.read
      .format("com.databricks.spark.csv")
      .schema(schema_2)
      .option("header", "true")
      .option("sep", "\t")
      .option("inferSchema", "false")
      .load("C:\\Users\\Administrateur\\Desktop\\data\\orderLine.txt")
    // visualisation des schémas
    println("Schema lines")
    df_orders_lines.printSchema()
    println("Schema orders")
    df_orders_2.printSchema()
    println()
    // visualisation du tableau final
    println("Résultat")
    val test = df_orders_2.join(df_orders_lines, df_orders_lines("orderid") === df_orders_2("orderid"), joinType = "inner")
    test.show()


  }

}
