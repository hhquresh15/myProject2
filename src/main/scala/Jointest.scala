import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD

object Jointest {


  case class cus( unique_id:Int, timestamp:String, CusId:Int)
  {}

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/spark/winutils")
    System.setProperty("spark.sql.warehouse.dir","C:/spark")

    val conf = new SparkConf().setMaster("local[4]").setAppName("MyApp")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //val l1 = List((1,"2012-09-03",332),(2,"2013-09-03",334),(3,"2012-04-08",333),(4,"2012-04-04",666))
    val l1 = Seq((1,"2012-09-03",332),(2,"2013-09-03",334),(3,"2012-04-08",333),(4,"2012-04-04",666))
    //val listrdd = sc.parallelize(l1)

    val df1 = l1.toDF("unique_id","timestamp","customer_id")
    //val listrdd =l1.toSeq.toDF("unique_id","timestamp","customer_id")
    //listrdd.show()

    val c1=cus(1,"2016-08-03",8)
    val c2=cus (7,"2018-08-03",9)
    val c3=cus (8,"2017-08-03",10)
    val c4 = List(c1,c2,c3)
    //val list = sc.parallelize(List(("a1","b1","c1","d1"),("a2","b2","c2","d2"))).toDF
    val custs = sc.parallelize(c4)
    val customersDF=custs.toDF()
    //Method one
    //customersDF.join(df1,customersDF("unique_id")===df1("unique_id")).select(customersDF("timestamp"),df1("timestamp")).show()
    //custs.foreach(println)
     //method 2
    //customersDF.join(df1,"unique_id").select(customersDF("timestamp"),df1("timestamp")).show()
    customersDF.join(df1,"unique_id").show()


    //val customerDF=sc.parallelize(c4).toDF()
    //cRdd.foreach(println)

  }
}
