import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object query7 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Region-wise Employee Count with Partitioning")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // Load the CSV file as RDD
    val rdd = sc.textFile("C:\\Users\\vgoyal\\Downloads\\BANKDATA.csv")

    // Extract header and filter it out
    val header = rdd.first()
    val dataRDD = rdd.filter(row => row != header)

    // Repartition the RDD for parallel processing
    val partitionedRDD = dataRDD.repartition(4)

    // Extract COMPANY LOCATION (assumed to be column index 5)
    val locationCounts = partitionedRDD
      .map(_.split(",", -1))
      .filter(fields => fields.length > 5)
      .map(fields => fields(5).trim)
      .map(location => (location, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    // Convert to DataFrame
    val rowRDD = locationCounts.map { case (location, count) => Row(location, count.toString) }

    val schema = StructType(List(
      StructField("COMPANY_LOCATION", StringType, nullable = true),
      StructField("EMPLOYEE_COUNT", StringType, nullable = true)
    ))

    val df = spark.createDataFrame(rowRDD, schema)

    // Save as a single CSV file
    df.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv("C:\\Users\\vgoyal\\Downloads\\location_counts_single")

    spark.stop()
  }
}
