import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object partitionexample {
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
    val rdd1 = sc.textFile("C:\\Users\\hassan.qureshi\\Documents\\people.csv", 2)
    val header = rdd1.first()
    //val datardd = rdd1.filter(line => line!=header).map(line =>line.split("\\,")).filter(col =>col(3)!="Toronto")
    //datardd.foreach(cols=>println(cols(0)+ " " + cols(1) + " " +cols(2) + " " +cols(3)))

    def func(i:List[String]):String =
    {
      i.mkString(",")
    }


    //val datardd = rdd1.filter(line => line!=header).map(line =>line.split("\\,")).filter(col =>col(3)!="Toronto").map(cols=>func(cols.toList))

    val datardd = rdd1.filter(line => line!=header).map(line =>line.split("\\,")).filter(col =>col(3)!="Toronto").map(cols=>cols.mkString(","))
    val dd=datardd.repartition(5)
    dd.foreach(println)

  }
}
