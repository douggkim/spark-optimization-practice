package part2foundations

import org.apache.spark.sql.SparkSession

object TestDeploy {
  // TestDeployApp inputFile outputFile
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Nedd input file and output file")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("Test Deploy App")
      .getOrCreate()

    import spark.implicits._

    val moveisDF = spark.read.option("inferSchema", "true")
      .json(args(0))

    val goodCommediesDF = moveisDF.select(
        $"Title",
        $"IMDB_Rating".as("Rating"),
        $"Release_Date".as("Release")
      ).where($"Major_Genre" === "Comedy"
        && $"IMDB_Rating" > 6.5)
      .orderBy($"Rating".desc_nulls_last)

    goodCommediesDF.show()

    goodCommediesDF.write.mode("overwrite").format("json").save(args(1))

  }

}
