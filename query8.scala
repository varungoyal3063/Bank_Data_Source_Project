import org.apache.spark.sql.{SparkSession, functions => F}

object query8 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Experience Based Offers")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Load the CSV file
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\vgoyal\\Downloads\\BANKDATA.csv")

    // Categorize experience and assign offers
    val resultDF = df.withColumn("Experience_Category",
      F.when(F.col("EXPERIENCE") <= 2, "0-2 years")
        .when(F.col("EXPERIENCE") <= 5, "2-5 years")
        .when(F.col("EXPERIENCE") <= 10, "5-10 years")
        .otherwise("10+ years")
    ).withColumn("Offer",
      F.when(F.col("Experience_Category") === "0-2 years", "Welcome Kit + 5% Cashback on first purchase")
        .when(F.col("Experience_Category") === "2-5 years", "Silver Membership + 10% Cashback")
        .when(F.col("Experience_Category") === "5-10 years", "Gold Membership + 15% Cashback")
        .otherwise("Platinum Membership + 20% Cashback + VIP Support")
    )

    // Select relevant columns
    val finalDF = resultDF.select("EMP_NAME", "EXPERIENCE", "Experience_Category", "Offer")

    // Show the result
//    finalDF.show(truncate = false)

    // Optionally save the result
    finalDF.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv("C:\\Users\\vgoyal\\Downloads\\experience_offers")

    spark.stop()
  }
}
