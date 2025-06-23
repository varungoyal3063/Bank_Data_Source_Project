import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.types.DoubleType

object query4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Income Tax Savings Calculation")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Load the CSV file
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\vgoyal\\Downloads\\BANKDATA.csv")

    // Define tax rate
    val taxRate = 0.10

    // Step 1: Remove rows with null or empty EMP_NAME
    val cleanedDF = df.filter(F.col("EMP_NAME").isNotNull && F.trim(F.col("EMP_NAME")) =!= "")

    // Step 2: Convert BALANCE to INCOME and calculate TAX_SAVINGS
    val resultDF = cleanedDF
      .withColumn("INCOME", F.col("BALANCE").cast(DoubleType))
      .withColumn("TAX_SAVINGS",
        F.when(F.col("INCOME") > 200000, (F.col("INCOME") - 200000) * taxRate)
          .otherwise(0.0)
      )
      .select($"EMP_NAME", $"INCOME", $"TAX_SAVINGS")
      .filter(F.col("INCOME").isNotNull) // Optional: remove rows where INCOME is null

    // Step 3: Save the cleaned and processed data
    resultDF.write
      .option("header", "true")
      .mode("overwrite")
      .csv("C:\\Users\\vgoyal\\Downloads\\income_tax_savings_df")

    spark.stop()
  }
}
