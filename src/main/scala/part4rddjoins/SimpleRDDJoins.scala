package part4rddjoins

import generator.DataGenerator
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SimpleRDDJoins {

  val spark = SparkSession.builder()
    .appName("RDD joins")
    .master("local[*]")
    .config("spark.driver.host", "localhost")
    .getOrCreate()

  val sc = spark.sparkContext
  val rootFolder = "src/main/resources/generated/examData"
  // for generating dummy data
  //  DataGenerator.generateExamData(rootFolder, 1000000, 5)

  def readIds() = sc.textFile(s"${rootFolder}/examIds.txt")
    .map {
      line =>
        val tokens = line.split(" ")
        (tokens(0).toLong, tokens(1))
    }
    .partitionBy(new HashPartitioner(10))

  def readExamScores() = sc.textFile(s"${rootFolder}/examScores.txt")
    .map {
      line =>
        val tokens = line.split(" ")
        (tokens(0).toLong, tokens(1).toDouble)
    }

  // goal: the number of students who passed the exam (= at least one attempt > 9.0)

  def plainJoin() = {
    val candidates = readIds()
    val examScores = readExamScores()

    // simple join
    val joined: RDD[(Long, (Double, String))] = examScores.join(candidates) // (score attempt, candidate name)
    val finalScores = joined
      .reduceByKey((pair1, pair2) => if (pair1._1 > pair2._1) pair1 else pair2) // compare all attemps and only keep the largest
      .filter(_._2._1 > 9.0)

    finalScores.count()
  }

  def preAggregate() = {
    val candidates = readIds()
    val examScores = readExamScores()

    // do aggregation first
    val maxScores: RDD[(Long, Double)] = examScores.reduceByKey(Math.max)
    val finalScores = maxScores.join(candidates)
      .filter(_._2._1>9.0)


    finalScores.count()
  }

  def preFiltering()= {
    val candidates = readIds()
    val examScores = readExamScores()

    // do aggregation and filtering first before the join
    val maxScores: RDD[(Long, Double)] = examScores.reduceByKey(Math.max).filter(_._2 > 9.0)
    val finalScores = maxScores.join(candidates)

    finalScores.count()
  }

  def coPartitioning() = {
    val candidates = readIds()
    val examScores = readExamScores()

    val partitionerForScores = candidates.partitioner match {
      case None => new HashPartitioner(candidates.getNumPartitions)
      case Some(partitioner) => partitioner
    }
    val repartitionScores = examScores.partitionBy(partitionerForScores)
    // simple join
    val joined: RDD[(Long, (Double, String))] = repartitionScores.join(candidates) // (score attempt, candidate name)
    val finalScores = joined
      .reduceByKey((pair1, pair2) => if (pair1._1 > pair2._1) pair1 else pair2) // compare all attemps and only keep the largest
      .filter(_._2._1 > 9.0)
    finalScores.count()
  }

  def combinedOptimization() = {
    val candidates = readIds()
    val examScores = readExamScores()

    val partitionerForScores = candidates.partitioner match {
      case None => new HashPartitioner(candidates.getNumPartitions)
      case Some(partitioner) => partitioner
    }
    val repartitionScores = examScores.partitionBy(partitionerForScores)
    // do aggregation and filtering first before the join
    val maxScores: RDD[(Long, Double)] = repartitionScores.reduceByKey(Math.max).filter(_._2 > 9.0)
    val finalScores = maxScores.join(candidates)
    finalScores.count()
  }



  def main(args: Array[String]): Unit = {
    plainJoin()
    preAggregate()
    preFiltering()
    coPartitioning()
    combinedOptimization()

    Thread.sleep(1000000)

  }

}
