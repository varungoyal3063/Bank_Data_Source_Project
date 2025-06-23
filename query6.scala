import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SparkSession, functions => F}

object RewardOffers {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Reward Tier Offers")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Load the CSV file
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\vgoyal\\Downloads\\BANKDATA.csv")

    // Clean and cast REWARD POINTS column
    val cleanedDF = df.withColumn("REWARD_POINTS", F.col("`REWARD POINTS `").cast(IntegerType))

    // Categorize into reward tiers
    val withTiers = cleanedDF.withColumn("Reward_Tier",
      F.when(F.col("REWARD_POINTS").between(0, 999), "Bronze")
        .when(F.col("REWARD_POINTS").between(1000, 4999), "Silver")
        .when(F.col("REWARD_POINTS").between(5000, 9999), "Gold")
        .when(F.col("REWARD_POINTS") >= 10000, "Platinum")
        .otherwise("Unknown")
    )

    // Add personalized offers
    val withOffers = withTiers.withColumn("Offer",
      F.when(F.col("Reward_Tier") === "Bronze", "5% off on next purchase")
        .when(F.col("Reward_Tier") === "Silver", "10% off + free shipping")
        .when(F.col("Reward_Tier") === "Gold", "15% off + priority support")
        .when(F.col("Reward_Tier") === "Platinum", "20% off + VIP lounge access")
        .otherwise("No offer available")
    )

    // Select relevant columns
    val resultDF = withOffers.select("EMP_NAME", "REWARD_POINTS", "Reward_Tier", "Offer")

    // Show the result
    //resultDF.show(truncate = false)

    // Optionally save the result
    resultDF.write
      .option("header", "true")
      .mode("overwrite")
      .csv("C:\\Users\\vgoyal\\Downloads\\reward_offers_output")

    spark.stop()
  }
}
