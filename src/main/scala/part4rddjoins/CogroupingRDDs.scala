package part4rddjoins

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import part4rddjoins.SimpleRDDJoins.{rootFolder, sc}

object CogroupingRDDs {

  val spark = SparkSession.builder()
    .appName("Cogrouping RDDs")
    .master("local[*]")
    .config("spark.driver.host", "localhost")
    .getOrCreate()

  val sc = spark.sparkContext

  /*
  * Take all the student attempts
  * - if a student passed (at least one attempt > 9.0), send them an email 'PASSED'
  * - else sendThem an email with "FAILED"
  * */
  val rootFolder = "src/main/resources/generated/examData"

  def readIds() = sc.textFile(s"${rootFolder}/examIds.txt")
    .map {
      line =>
        val tokens = line.split(" ")
        (tokens(0).toLong, tokens(1))
    }


  def readExamScores() = sc.textFile(s"${rootFolder}/examScores.txt")
    .map {
      line =>
        val tokens = line.split(" ")
        (tokens(0).toLong, tokens(1).toDouble)
    }

  def readExamEmails() = sc.textFile(s"${rootFolder}/examEmails.txt")
    .map {
      line =>
        val tokens = line.split(" ")
        (tokens(0).toLong, tokens(1))
    }


  def plainJoin() = {
    val scores = readExamScores().reduceByKey(Math.max)
    val candidates = readIds()
    val emails = readExamEmails()

    val results = candidates
      .join(scores) // RDD[(Long, (String, Double))] Candidate ID, name, score
      .join(emails) // RDD[(Long), ((String,Double), String)]
      .mapValues {
        case ((name, maxAttempt), email) =>
          if (maxAttempt >= 9.0) (email, "PASSED")
          else (email, "FAILED")
      }

    results.count()
  }

  def coGroupedJoin() = {
    val scores = readExamScores().reduceByKey(Math.max)
    val candidates = readIds()
    val emails = readExamEmails()

    // Cogroup: similar to join, but make sure the three RDDs share the partitioner
    // Will be a full outer join
    // results in Iterable => RDD[(Long, (Iterable[String], Iterable[Double], Iterable[String]))]
    // above seems like key and all the other columns as Iterables
    val results: RDD[(Long, Option[(String, String)])] = candidates.cogroup(scores, emails)
      .mapValues {
        case (nameIterable, maxAttemptIterable, emailIterable) =>
          val name = nameIterable.headOption
          val maxScore = maxAttemptIterable.headOption
          val email = emailIterable.headOption

          for {
            e <- email
            s <- maxScore
          } yield (e, if (s >= 9.0) "PASSED" else "FAILED")
      }

    results.count()
    results.count()
  }

  def main(args: Array[String]): Unit = {

    plainJoin()
    coGroupedJoin()
    Thread.sleep(1000000)
  }

}
