package part4rddjoins

import generator.{DataGenerator, Laptop, LaptopOffer}
import org.apache.spark.sql.SparkSession

object RDDSkewedJoins {
  val spark = SparkSession.builder()
    .appName("RDD Skewed Joins")
    .master("local[*]")
    .config("spark.driver.host", "localhost")
    .getOrCreate()

  val sc = spark.sparkContext

  /*
    An online store selling gaming laptops.
    2 laptops are "similar" if they have the same make & model, but proc speed within 0.1

    For each laptop configuration, we are interested in the average sale price of "similar" models.

    Acer Predator 2.9Ghz aylfaskjhrw -> average sale price of all Acer Predators with CPU speed between 2.8 and 3.0 GHz
   */

  val laptops = sc.parallelize(Seq.fill(40000)(DataGenerator.randomLaptop()))
  val laptopOffers = sc.parallelize(Seq.fill(100000)(DataGenerator.randomLaptopOffer()))

  def plainJoin(): Unit = {
    val preparedLaptops = laptops.map {
      case Laptop(registration, make, model, procSpeed) => ((make, model), (registration, procSpeed))
    }

    val preparedOffers = laptopOffers.map {
      case LaptopOffer(make, model, procSpeed, salePrice) => ((make, model), (procSpeed, salePrice))
    }

    val result = preparedLaptops.join(preparedOffers) // RDD[(make, model), ((registration, cpu), (cpu, salePrice))]
      .filter {
        case ((make, model), ((reg, laptopCpu), (offerCpu, salePrice))) => Math.abs(laptopCpu - offerCpu) <= 0.1
      }
      .map {
        case ((make, model), ((reg, laptopCpu), (offerCpu, salePrice))) => (reg, salePrice)
      }
      .aggregateByKey((0.0, 0))( //starting point
        {
          case ((totalPrice, numPrices), salePrice) => ((totalPrice + salePrice, numPrices + 1)) // combine state with record -> Inside each partition
        },
        {
          case ((totalPrice1, numPrices1), (totalPrices2, numPrices2)) => (totalPrice1 + totalPrices2, numPrices1 + numPrices2) // combine two states into 1 -> Across partitions
        }
      ) // RDD[(String, (Double, Int))]
      .mapValues {
        case (totalPrices, numPrices) => totalPrices / numPrices
      }
    // (0.0, 0) : price so far, total number of sales so far
    //      .groupBykey()
    //      .mapValues(prices => prices.sum/ prices.size)= > slow, so we're using aggregateByKey
    // groupBy(reg).avg(salePrice)

    result.count()

  }

  def noSkewJoin() = {
    val preparedLaptops = laptops.flatMap {
      laptop =>
        Seq(
          laptop,
          laptop.copy(procSpeed = laptop.procSpeed - 0.1),
          laptop.copy(procSpeed = laptop.procSpeed + 0.1),
        )

    }.map {
      case Laptop(registration, make, model, procSpeed) => ((make, model, procSpeed), procSpeed)
    }

    val preparedOffers = laptopOffers.map {
      case LaptopOffer(make, model, procSpeed, salePrice) => ((make, model, procSpeed), salePrice)
    }

    val result = preparedLaptops.join(preparedOffers) // RDD[(make,model, procSpeed), (reg, salesPrice)]
      .map(_._2) // select RDD[(reg, salesPrice)]
      .aggregateByKey((0.0, 0))( //starting point
        {
          case ((totalPrice, numPrices), salePrice) => ((totalPrice + salePrice, numPrices + 1)) // combine state with record -> Inside each partition
        },
        {
          case ((totalPrice1, numPrices1), (totalPrices2, numPrices2)) => (totalPrice1 + totalPrices2, numPrices1 + numPrices2) // combine two states into 1 -> Across partitions
        }
      ) // RDD[(String, (Double, Int))]
      .mapValues {
        case (totalPrices, numPrices) => totalPrices / numPrices
      }

    result.count()


  }

  def main(args: Array[String]): Unit = {
    plainJoin()
    noSkewJoin()
    Thread.sleep(1000000)
  }


}
