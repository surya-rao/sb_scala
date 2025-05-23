import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object spark_test {
  def main(args: Array[String]): Unit = {

    val sc=new SparkContext("local[*]","Word Count in Spark")
    val rdd1=sc.textfile("C:/Users/surya/OneDrive/Desktop/data.txt")
    val rdd2=rdd1.flatMap(x=>x.split(" "))
    val rdd3 = rdd2.map(x=>(x,1))
    val rdd4=rdd3.ReduceByKey((x,y)=>x+y)
    rdd4.collect.foreach(println)
  }
}
