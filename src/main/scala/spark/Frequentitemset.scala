package spark

import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import spark.Frequentitemset.Itemset
import spark.Util.absoluteSupport

import scala.collection.mutable

object Frequentitemset {

  type Itemset = List[String]

  def main(args: Array[String]): Unit = {
    val fim = new Apriori
    val frequentSets = fim.execute("src/main/resources/datasets/mushroom.txt", " ", 0.35)
  }
}

/**
 * 1. Generate singletons
 * 2. Find K+1 frequent itemsets
 *
 */
class Apriori extends FIM with Serializable {

  def generateSingeltons(fileName: String, separator: String, transactions: List[Itemset], minSupport: Double): List[Itemset] = {
    var spark: SparkSession = null
    val appName = Util.appName
    spark = SparkSession.builder()
      .appName(appName)
      .master("local[4]")
      //.config("spark.eventLog.enabled", "true")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val t0 = System.currentTimeMillis()

    var transactionsRDD: RDD[Itemset] = null
    var support: Int = 0

    if (!fileName.isEmpty) {
      // Fetch transaction
      val file = List.fill(Util.replicateNTimes)(fileName).mkString(",")
      var fileRDD: RDD[String] = null
      if (Util.minPartitions == -1)
        fileRDD = sc.textFile(file)
      else
        fileRDD = sc.textFile(file, Util.minPartitions)

      transactionsRDD = fileRDD.filter(!_.trim.isEmpty)
        .map(_.split(separator + "+"))
        .map(l => l.map(_.trim).toList)

      if (Util.props.getProperty("fim.cache", "false").toBoolean) {
        transactionsRDD = transactionsRDD.cache()
        println("cached")
      }
      support = absoluteSupport(minSupport, transactionsRDD.count().toInt)
    }
    else {
      transactionsRDD = sc.parallelize(transactions)
      support = absoluteSupport(minSupport, transactions.size)
    }

    // Generate singletons
    val singletonsRDD = transactionsRDD
      .flatMap(identity)
      .map(item => (item, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= support)

    val frequentItemsets = findFrequentItemsets(transactionsRDD, singletonsRDD, support, spark, sc)

    executionTime = System.currentTimeMillis() - t0

    if (Util.props.getProperty("fim.unpersist", "false").toBoolean) {
      transactionsRDD.unpersist()
      println("unpersited")
    }

    if (Util.props.getProperty("fim.closeContext", "false").toBoolean) {
      spark.sparkContext.stop()
      println("stopped")
    }

    frequentItemsets
  }

  def findFrequentItemsets(transactions: RDD[Itemset], singletons: RDD[(String, Int)], minSupport: Int,
                           spark: SparkSession, sc: SparkContext): List[Itemset] = {

    // Creating a file
    val file_Object = new File("output.txt" )

    // Passing reference of file to the printwriter
    val print_Writer = new PrintWriter(file_Object)

    // Closing printwriter

    val frequentItemsets = mutable.Map(1 -> singletons.map(_._1).map(List(_)).collect().toList)
    print_Writer.write(s"Number of singletons: ${frequentItemsets(1).size} \n")
    var k = 1
    while (frequentItemsets.get(k).nonEmpty) {
      k += 1

      // Candidate generation and initial pruning
      val candidates = candidateGeneration(frequentItemsets(k - 1), sc)

      // Final filter by checking with all transactions
      val kFrequentItemsets = filterFrequentItemsets(candidates, transactions, minSupport, sc)
      if (kFrequentItemsets.nonEmpty) {
        frequentItemsets.update(k, kFrequentItemsets)
        print_Writer.write(s"Number of itemsets with size $k: ${frequentItemsets(k).size} \n")
//        println(s"Number of itemsets with size $k: ${frequentItemsets(k).size}")
        frequentItemsets(k).map( list => {
          print_Writer.write("{")
          list.map( item =>  print_Writer.write(s"${item} "))
          print_Writer.write("}\n")
        }
        )
      }
    }
    print_Writer.close()
    frequentItemsets.values.flatten.toList
  }

  def candidateGeneration(frequentSets: List[Itemset], sc: SparkContext) = {

    val previousFrequentSets = sc.parallelize(frequentSets)
    val cartesian = previousFrequentSets.cartesian(previousFrequentSets)
      .filter { case (a, b) =>
        a.mkString("") > b.mkString("")
      }
    cartesian
      .flatMap({ case (a, b) =>
        var result: List[Itemset] = null
        if (a.size == 1 || allElementsEqualButLast(a, b)) {
          val newItemset = (a :+ b.last).sorted
          if (isItemsetValid(newItemset, frequentSets))
            result = List(newItemset)
        }
        if (result == null) List.empty[Itemset] else result
      })
      .collect().toList
  }

  def allElementsEqualButLast(a: List[String], b: List[String]): Boolean = {
    for (i <- 0 until a.size - 1) {
      if (a(i) != b(i))
        return false
    }
    if (a.last == b.last) {
      return false
    }
    true
  }

  /**
   * Performs pruning by checking if all subsets of the new itemset exist within
   * the k-1 itemsets.
   * Do all subsets need to be checked or only those containing n-1 and n-2?
   */
  def isItemsetValid(itemset: List[String], previousItemsets: List[Itemset]): Boolean = {
    for (i <- itemset.indices) {
      val subset = itemset.diff(List(itemset(i)))
      val found = previousItemsets.contains(subset)
      if (!found) {
        return false
      }
    }
    true
  }

  def filterFrequentItemsets(candidates: List[Itemset], transactionsRDD: RDD[Itemset], minSupport: Int, sc: SparkContext) = {

    val candidatesBC = sc.broadcast(candidates)
    val filteredCandidatesRDD = transactionsRDD.flatMap(t => {
      candidatesBC.value.flatMap(c => {
        // candidate exists within the transaction
        //if (t.intersect(itemset).size == itemset.size) { TODO: Why intersect so much slower?
        if (candidateExistsInTransaction(c, t))
          List(c)
        else
          List.empty[Itemset]
      })
    })

    filteredCandidatesRDD.map((_, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= minSupport)
      .map(_._1)
      .collect().toList
  }

  def candidateExistsInTransaction(candidate: Itemset, transaction: Itemset): Boolean = {
    // all elements in candidate exist in transaction
    var result = true
    for (elem <- candidate) {
      if (!transaction.contains(elem))
        result = false
    }
    result
  }

}