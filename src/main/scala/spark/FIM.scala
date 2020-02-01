package spark
import spark.Frequentitemset.Itemset

trait FIM {
  var executionTime: Long = 0

  def generateSingeltons(fileName: String = "", separator: String = "", transactions: List[Itemset], minSupport: Double): List[Itemset]

  def execute(fileName: String, separator: String, minSupport: Double): List[Itemset] = {
    executionTime = 0
    val t0 = System.currentTimeMillis()
    val itemsets = generateSingeltons(fileName, separator, List.empty, minSupport)
    if (executionTime == 0)
      executionTime = System.currentTimeMillis() - t0
    println(f"Elapsed time: ${executionTime / 1000d}%1.2f seconds. Class: ${getClass.getSimpleName}.")
    itemsets
  }

}
