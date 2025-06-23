import org.apache.spark.sql.SparkSession

object query1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Top 10 Bank Balances")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Load the CSV file
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\vgoyal\\Downloads\\BANKDATA.csv")

    // Convert balance to numeric and filter non-null
    val dfWithBalance = df
      .withColumn("BALANCE", $"BALANCE".cast("double"))
      .na.drop(Seq("BALANCE")) //cleaned the dataset

    // Get top 10 accounts by balance
    val topBalances = dfWithBalance.orderBy($"BALANCE".desc).limit(10)

    // Save to CSV

    topBalances.coalesce(1)
 .write
 .mode("overwrite")
    .parquet("C:\\Users\\vgoyal\\Downloads\\topbalances_output_parquet")

//parquet and orc are not readable , we have to create the dataframe for these file and then we can read
    spark.stop()
  }
}
