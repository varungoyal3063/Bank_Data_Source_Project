import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object query2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Company Account Holder Count with Partitioning")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Load CSV into DataFrame
    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\vgoyal\\Downloads\\BANKDATA.csv")
      .cache()

    // Clean and filter
    val cleanedDF = df.filter($"COMPANY".isNotNull && length(trim($"COMPANY")) > 0)

    // Repartition by COMPANY for optimized processing
    val partitionedDF = cleanedDF.repartition($"COMPANY")

    // Group by COMPANY and count
    val companyCounts = partitionedDF
      .groupBy("COMPANY")
      .agg(count("*").alias("Account_Holder_Count"))
      .orderBy(desc("Account_Holder_Count"))

    // Show result
   // companyCounts.show(false)

    // Optional: Save partitioned output
    companyCounts.write
      .option("header", "true")
      .partitionBy("COMPANY")
      .mode("overwrite")
      .csv("C:\\Users\\vgoyal\\Downloads\\company_counts_partitioned")

    spark.stop()
  }
}
