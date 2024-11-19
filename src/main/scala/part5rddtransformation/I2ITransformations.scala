package part5rddtransformation

import generator.DataGenerator
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object I2ITransformations {

  val spark = SparkSession.builder()
    .appName("Reusing JVM objects")
    .master("local[*]")
    .config("spark.driver.host", "localhost")
    .getOrCreate()

  val sc = spark.sparkContext

  /* Science project
  * each metric has an identifier, and a value
  *
  * Return the smallest ("best") 10 metrics with identifiers and values
  * */
  val metricRootPath = "src/main/resources/generated/metrics/"
  val LIMIT = 10
  //  DataGenerator.generateMetrics(metricRootPath +"metrics10m.txt", 10000000)


  def readMetrics() = sc.textFile(metricRootPath + "metrics10m.txt")
    .map { line =>
      val tokens = line.split(" ")
      val name = tokens(0)
      val value = tokens(1)

      (name, value.toDouble)
    }

  def printTopMetrics() = {
    val sortedMetrics = readMetrics().sortBy(_._2).take(LIMIT) //
    sortedMetrics.foreach(println)
  }

  def printTopMetricsI2I() = {

    val iteratorToIteratorTransformation = (records: Iterator[(String, Double)]) => {
      /*
        i2i transformation
        - they are NARROW TRANSFORMATIONS
        - Spark will "selectively" spill data to disk when partitions are too big for memory

        Warning: don't traverse more than once or convert to collections
        */

      implicit val ordering: Ordering[(String, Double)] = Ordering.by[(String, Double), Double](_._2)
      val limitedCollection = new mutable.TreeSet[(String, Double)]()

      records.foreach { record =>
        limitedCollection.add(record)
        if (limitedCollection.size > LIMIT) {
          limitedCollection.remove(limitedCollection.last)
        }
      }

      // I've traversed the iterator

      limitedCollection.iterator
    }

    val topMetrics = readMetrics()
      .mapPartitions(iteratorToIteratorTransformation)
      .repartition(1)
      .mapPartitions(iteratorToIteratorTransformation)

    val result = topMetrics.take(LIMIT)
    result.foreach(println)
  }


  /*Exercises
  *
  * Ex1
  * Better than he "dummy" approach
  * - not sorting the entire RDD
  *
  * Bad
  * - sorting the entire partition <-> I2I does sorting, but it only sorts LIMIT number of elements maximum.
  * - forcing the iterator in memory - could cause OOM
  * */
  def printTopMetricsEx1() = {
    val topMetrics = readMetrics()
      .mapPartitions(_.toList.sortBy(_._2).take(LIMIT).toIterator)
      .repartition(1)
      .mapPartitions(_.toList.sortBy(_._2).take(LIMIT).toIterator)
      .take(LIMIT)

    topMetrics.foreach(println)
  }

  /*
    Better than ex1
    - extracting top 10 values per partition instead of sorting the entire partition

    Bad because
    - forcing toList can OOM your executors
    - iterating over the list twice (one for toList, one for foreach)
    - if the list is immutable, time spent allocating objects (and GC) for each partition could be an overhead
   */

  def printTopMetricsEx2() = {
    val topMetrics = readMetrics()
      .mapPartitions { records =>

        implicit val ordering: Ordering[(String, Double)] = Ordering.by[(String, Double), Double](_._2)
        val limitedCollection = new mutable.TreeSet[(String, Double)]()

        records.toList.foreach { record =>
          limitedCollection.add(record)
          if (limitedCollection.size > LIMIT) {
            limitedCollection.remove(limitedCollection.last)
          }
        }

        // I've traversed the iterator

        limitedCollection.iterator
      }
      .repartition(1)
      .mapPartitions { records =>

        implicit val ordering: Ordering[(String, Double)] = Ordering.by[(String, Double), Double](_._2)
        val limitedCollection = new mutable.TreeSet[(String, Double)]()

        records.toList.foreach { record =>
          limitedCollection.add(record)
          if (limitedCollection.size > LIMIT) {
            limitedCollection.remove(limitedCollection.last)
          }
        }

        // I've traversed the iterator

        limitedCollection.iterator
      }
      .take(LIMIT)

    topMetrics.foreach(println)
  }

  def main(args: Array[String]): Unit = {
    printTopMetrics()
    printTopMetricsI2I()
    Thread.sleep(10000000)

  }

}
