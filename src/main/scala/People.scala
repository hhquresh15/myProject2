import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql._
import scala.util.matching.Regex
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.FloatType


object People {

  def defType(i:String):Any={
    if (i.contains(".")) FloatType
    else StringType
  }

  def replacecommas(i: String): String = {
    val pattern = new Regex("\\d+,\\d+")
    val word=(pattern findAllIn i).mkString(",")
    //println("1."+word)
    val newWord=word.replace(",","")
    //println("2."+newWord)
    i.replaceAll("\\d+,\\d+", newWord)


    }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/spark/winutils")
    System.setProperty("spark.sql.warehouse.dir","C:/spark")
    val conf = new SparkConf().setMaster("local[4]").setAppName("MyApp")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("C:\\Users\\hassan.qureshi\\Documents\\people.csv")
    //df.show()
    //df.printSchema()
    val rdd1 = sc.textFile("C:\\Users\\hassan.qureshi\\Documents\\people.csv")
    val header = rdd1.first()
    //rdd1.foreach(println)
    val noheader = rdd1.filter(line => line!=header)
   //val changetoRow = noheader.map(line=>replacecommas(line))
   val changetoRow = noheader.map(line=>replacecommas(line)).map(line => line.split("\\,")).map(row => Row(row(0),row(1),row(2).replace("\"",""),row(3)))
    changetoRow.foreach(println)
    //println("3."+replacecommas("ablaw 6887,89 is abla1 and cool"))
//    val schema = StructType(header.split(",").map(col=>StructField(col,(DataTypes) defType(col), true)))
//    val df=sqlContext.createDataFrame(changetoRow,schema)
//    df.show()
//    df.printSchema()

    println(defType("567.6"))
  }
}
