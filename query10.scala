import org.apache.spark.sql.SparkSession

import java.io._

object query10 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Loan Analysis using RDD")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // Load the CSV file as text
    val data = sc.textFile("C:\\Users\\vgoyal\\Downloads\\BANKDATA.csv")

    // Extract header and remove it from the data
    val header = data.first()
    val rows = data.filter(_ != header)

    // Split each row by comma and extract relevant columns
    // MARITAL STATUS = index 2, HOUSING = index 14, Loan = index 15
    val analysisRDD = rows.map(_.split(",", -1))
      .filter(fields => fields.length > 15)
      .map(fields => ((fields(2).trim, fields(14).trim, fields(15).trim), 1))
      .reduceByKey(_ + _)
      .map { case ((marital, housing, loan), count) =>
        s"$marital,$housing,$loan,$count"
      }

    // Save the result to local file system
    val outputPath = "C:\\Users\\vgoyal\\Downloads\\loan_analysis_rdd_output"
    analysisRDD.coalesce(1).saveAsTextFile(outputPath)

    println(s"Loan analysis saved to: $outputPath")

    spark.stop()
  }
}
