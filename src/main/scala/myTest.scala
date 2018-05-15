import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


object myTest {

  //(3,"Toyota-Camry","Red")
  //(1,"Nissan-Altima","Black")
  //(2,"Nissan-Maxima","White")
  //(4,"Toyota-Corola","White")

  //How many cars for Toyota,Nissan
  // How many cars for each color


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:/spark/winutils")
    System.setProperty("spark.sql.warehouse.dir","C:/spark")

    val conf = new SparkConf().setMaster("local[4]").setAppName("MyApp")
    val sc = new SparkContext(conf)

    val l1 = List((3,"Toyota-Camry","Red"),(1,"Nissan-Altima","Black"),(2,"Nissan-Maxima","White"),(4,"Toyota-Corola","White"))
    val listrdd = sc.parallelize(l1)
    listrdd.foreach(println)
     val resultMap = listrdd.map(f =>(f._1,f._2)).map(f=>(f._2.substring(0,f._2.indexOf("-")),f._1)).reduceByKey(_+_)
    resultMap.foreach(println)
    //val resultReduce = resultMap.reduce
    //resultReduce.foreach(println)
    val resultMap2 =listrdd.map(f=>(f._3,f._1)).reduceByKey(_+_)
    resultMap2.foreach(println)
    println(sc.appName)
    println("Completed")
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  }
}

