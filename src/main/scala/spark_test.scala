import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._

object spark_test {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[4]","spark-program")

    val rdd1 = sc.textFile("C:/Users/surya/OneDrive/Desktop/data.txt")
//    //rdd1.collect.foreach(println)
    val rdd2 = rdd1.flatMap(x=>x.split(" "))
//    //rdd2.collect.foreach(println)

    val rdd3 = rdd2.map(x=>(x,1))
    //rdd3.collect.foreach(println)

    val rdd4 = rdd3.reduceByKey((x,y)=>x+y)

    //rdd4.collect.foreach(println)

    val rdd5=rdd4.sortBy(x=>x._2,false)   // sort by value in descending (false)

    //rdd5.collect.foreach(println)
    rdd5.collect.foreach(println)
//    rdd5.take(1).foreach(println)

    scala.io.StdIn.readLine("Press Enter to exit...")

    sc.stop()
  }
}
