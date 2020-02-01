package spark

import java.util.Properties

import spark.Frequentitemset.{Itemset}
import scala.io.Source

object Util {

  var replicateNTimes: Int = 1
  var minPartitions: Int = 8
  var appName = "FIM"
  var props: Properties = new Properties()

  def absoluteSupport(minSupport: Double, numTransactions: Int) = (numTransactions * minSupport + 0.5).toInt

  def parseTransactions(lines: List[String], separator: String): List[Itemset] = {
    lines.filter(l => !l.startsWith("#"))
      .filter(!_.trim.isEmpty)
      .map(l => l.split(separator + "+"))
      .map(l => l.map(item => item.trim).toList)
  }

  def parseTransactions(fileName: String, separator: String = ","): List[Itemset] = {

    parseTransactions(
      (1 to replicateNTimes).flatMap(_ => {
        val file = Source.fromFile(fileName, "UTF-8")
        file.getLines
      }).toList, separator)
  }

  def parseTransactionsByText(text: String): List[Itemset] = {
    parseTransactions(text.split("\n").toList, ",")
  }

}
