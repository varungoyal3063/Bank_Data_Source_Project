import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object query3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Accounts Opened Per Year with SQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\vgoyal\\Downloads\\BANKDATA.csv")

    // Filter rows with valid date format (dd-MM-yyyy)
    val validDateDF = df.filter($"`ACCOUNT OPENING DATE`".rlike("^\\d{2}-\\d{2}-\\d{4}$"))

    val dfWithDate = validDateDF.withColumn(
      "ACCOUNT_OPENING_DATE_PARSED",
      to_date($"`ACCOUNT OPENING DATE`", "dd-MM-yyyy")
    )

    dfWithDate.createOrReplaceTempView("bankdata")

    val result = spark.sql(
      """
        |SELECT year(ACCOUNT_OPENING_DATE_PARSED) AS YEAR, COUNT(*) AS Account_Count
        |FROM bankdata
        |WHERE ACCOUNT_OPENING_DATE_PARSED IS NOT NULL
        |GROUP BY year(ACCOUNT_OPENING_DATE_PARSED)
        |ORDER BY YEAR
        |""".stripMargin)

//result.show(false)

    result.write
      .option("header", "true")
      .partitionBy("YEAR")
      .mode("overwrite")
      .csv("C:\\Users\\vgoyal\\Downloads\\account_per_year.csv")
//    result.write
//    .option("header", "true")
// .partitionBy("COMPANY")
//   .mode("overwrite")
// .csv("C:\\Users\\vgoyal\\Downloads\\accounts_per_year.csv")

    spark.stop()
  }
}
