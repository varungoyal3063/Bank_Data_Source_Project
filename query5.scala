import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SparkSession, functions => F}

object query5{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Retirement Policy Categorization with Names")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Load the CSV file
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\vgoyal\\Downloads\\BANKDATA.csv")

    // Clean and cast EMP_AGE to Integer
    val cleanedDF = df.filter(F.col("EMP_AGE").isNotNull)
      .withColumn("EMP_AGE", F.col("EMP_AGE").cast(IntegerType))

    // Categorize based on age
    val categorizedDF = cleanedDF.withColumn("Retirement_Category",
      F.when(F.col("EMP_AGE") < 55, "Below Retirement Age")
        .when(F.col("EMP_AGE").between(55, 60), "Approaching Retirement")
        .otherwise("Eligible for Retirement")
    )

    // Select employee name and retirement category
    val resultDF = categorizedDF.select("EMP_NAME", "EMP_AGE", "Retirement_Category")

    // Show the result
    resultDF.show(truncate = false)

    // Optionally save the result
    resultDF.write
      .option("header", "true")
      .mode("overwrite")
      .csv("C:\\Users\\vgoyal\\Downloads\\retirement_summary")

    spark.stop()
  }
}
