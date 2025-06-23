

import org.apache.spark.sql.{SparkSession, DataFrame}

import org.apache.spark.sql.functions._

object query12 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()

      .appName("Partitioned Grouping by Age and Education")

      .master("local[*]")

      .getOrCreate()

    import spark.implicits._

    // Load dataset

    val df: DataFrame = spark.read

      .option("header", "true")

      .option("inferSchema", "true")

      .csv("C:\\Users\\vgoyal\\Downloads\\BANKDATA.csv")

      .cache()

    // Filter out nulls for AGE and EDUCATION
    val filteredDF = df.filter($"EMP_AGE".isNotNull && $"EDUCATION".isNotNull)
    // Group by EMP_AGE and EDUCATION and count
    val resultDF = filteredDF
      .groupBy($"EMP_AGE", $"EDUCATION")
      .agg(count("*").alias("Person_Count"))
      .orderBy($"EMP_AGE", $"EDUCATION")
    // Save output partitioned by EMP_AGE
    resultDF.write
      .option("header", "true")
      .partitionBy("EMP_AGE")
      .mode("overwrite")
      .csv("C:\\Users\\vgoyal\\Downloads\\age_education_partitioned")

    spark.stop()

  }

}
