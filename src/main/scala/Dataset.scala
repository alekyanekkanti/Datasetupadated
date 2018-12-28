
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Month, Year}

object Dataset{
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL example")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    val textFile = spark.read.format("csv").option("header","true").load("C:/alekya/online_retail.csv")

      val result = textFile.withColumn("sales",textFile("Quantity")multiply(textFile("UnitPrice")))

    result.show()
    val temp = result.groupBy($"Country").avg("sales").show()
    val temp1 = result.groupBy($"Country").min("sales").show()
    val temp2 = result.groupBy($"Country").max("sales").show()
    textFile.sort($"UnitPrice".asc).show()
    result.selectExpr("sales as user_sales").show()











  }

}
