import org.apache.spark.SparkContext

object spark_rdd_tests {
  def main(args: Array[String]): Unit = {

////    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//    val sc = new SparkContext("local[4]","spark-program")
//    val rdd1 = sc.textFile("C:/Users/surya/OneDrive/Desktop/data*.txt")
//    val partitionSizes = rdd1.glom().map(_.size).collect()
//    println("Number of partitions Before Filter and Contains: " + rdd1.getNumPartitions )
//    partitionSizes.zipWithIndex.foreach { case (size, idx) =>
//      println(s"Partition $idx has $size records")
//    }
//    val filtered = rdd1.filter(_.contains("instance"))
//    val reduced = filtered.coalesce(2)
//    println("Number of partitions After coalesce: " + reduced.getNumPartitions)
//    //reduced.saveAsTextFile("C:/Users/surya/OneDrive/Desktop/output.txt")
////    scala.io.StdIn.readLine("Press Enter to exit...")
//    sc.stop()
    //    //    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    val sc = new SparkContext("local[2]","Surya-app")
    val rdd1 = sc.textFile("C:/Users/surya/OneDrive/Desktop/data.txt")
    val rdd2 = rdd1.flatMap(x=>x.split(" "))
    val rdd3 = rdd2.map(x=>(x,1))
    val rdd4 = rdd3.reduceByKey((x,y)=>x+y)

    val rdd5 = rdd4.sortBy(x=>x._2,false)
//    rdd5.take(1)

//    rdd5.collect.foreach(println)
      rdd5.take(2).foreach(println)


    sc.stop()
    //    //    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


  }
}
