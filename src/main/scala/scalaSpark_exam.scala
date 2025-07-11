import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, lag, lead, rank, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

//object scalaSpark_exam {
//  def main(args: Array[String]): Unit = {
//
//    import org.apache.spark.sql.SparkSession
//    import org.apache.spark.sql.functions._
//
//    val spark = SparkSession.builder()
//      .appName("ConditionalColumnExample")
//      .master("local[*]")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    // Step 1: Create DataFrame
//    val employees = List(
//      (1, "AJAY", 28),
//      (2, "VIJAY", 35),
//      (3, "MANOJ", 22)
//    ).toDF("id", "name", "age")
//
//    // Step 2: Add conditional column using when/otherwise
//    val updatedDF = employees.withColumn(
//      "is_adult",
//      when(col("age") >= 18, lit(true)).otherwise(lit(false))
//    )
//
//    // Step 3: Show result
//    updatedDF.show()
//
//  }
//}

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//object scalaSpark_exam {
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder()
//      .appName("StudentDf")
//      .master("local[2]")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    import org.apache.spark.sql.Row
//
//    val studentSchema = StructType(Array(
//      StructField("student_id", IntegerType,true),
//      StructField("name", StringType,true),
//      StructField("score", IntegerType,true),
//      StructField("subject", StringType,true),
//    ))
//
//    val rowData = Seq(
//      Row(1, "Alice", 92, "Math"),
//      Row(2, "Bob", 85, "Math"),
//      Row(3, "Carol", 77, "Science"),
//      Row(4, "Dave", 65, "Science"),
//      Row(5, "Eve", 50, "Math"),
//      Row(6, "Frank", 82, "Science")
//    )
//
//    val studentDf2 = spark.createDataFrame(spark.sparkContext.parallelize(rowData),studentSchema)
////    studentDf2.show()
//
//    // Grade column based on score
//    import org.apache.spark.sql.functions.{when, col}
//
//    val studentWithGrade1 = studentDf2.withColumn("grade",
//      when(col("score") >= 90, "A")
//        .when(col("score") >= 80, "B")
//        .when(col("score") >= 70, "C")
//        .when(col("score") >= 60, "D")
//        .otherwise("F")
//    )
//
//    studentWithGrade1.show()
//
//  }
//}

////~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//
////~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//object scalaSpark_exam {
//  def main(args: Array[String]): Unit = {
//
//    val input_path=args(0)
//    val output_path=args(1)
//
////    println(input_path)
////    println(output_path)
//
//    val sc = new SparkContext("local[4]","spark-program")
//
//    val rdd1 = sc.textFile(input_path)
//    //    //rdd1.collect.foreach(println)
//    val rdd2 = rdd1.flatMap(x=>x.split(" "))
//    //    //rdd2.collect.foreach(println)
//
//    val rdd3 = rdd2.map(x=>(x,1))
//    //rdd3.collect.foreach(println)
//
//    val rdd4 = rdd3.reduceByKey((x,y)=>x+y)
//
//    //rdd4.collect.foreach(println)
//
//    val rdd5=rdd4.sortBy(x=>x._2,false)   // sort by value in descending (false)
//
//    rdd5.saveAsTextFile(output_path)
//    //rdd5.collect.foreach(println)
////    rdd5.collect.foreach(println)
//    //    rdd5.take(1).foreach(println)
//
////    scala.io.StdIn.readLine("Press Enter to exit...")
//
//    sc.stop()
//
//
//  }
//}
////~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//object scalaSpark_exam {
//  def main(args: Array[String]): Unit = {
//
////    val spark = SparkSession.builder()
////      .appName("SalaryTrendAnalysis")
////      .master("local[*]")
////      .getOrCreate()
////
////    import spark.implicits._
//
//    // Create sample data
////    val salaryData = Seq(
////      (1, "John", 1000, "01/01/2016"),
////      (1, "John", 2000, "02/01/2016"),
////      (1, "John", 1000, "03/01/2016"),
////      (1, "John", 2000, "04/01/2016"),
////      (1, "John", 3000, "05/01/2016"),
////      (1, "John", 1000, "06/01/2016")
////    ).toDF("ID", "NAME", "SALARY", "DATE")
////
////    val windowSpec = Window
////      .partitionBy("ID", "NAME")
////      .orderBy(to_date(col("DATE"), "MM/dd/yyyy"))
////
////    val salaryTrend1 = salaryData
////      .withColumn("DATE_PARSED", to_date(col("DATE"), "MM/dd/yyyy"))
////      .withColumn("PREVIOUS_SALARY", lag("SALARY", 1).over(windowSpec))
////      .withColumn("TREND",
////        when(col("PREVIOUS_SALARY").isNull, "FIRST")
////          .when(col("SALARY") > col("PREVIOUS_SALARY"), "UP")
////          .when(col("SALARY") < col("PREVIOUS_SALARY"), "DOWN")
////          .otherwise("SAME")
////      )
////      .select("ID", "NAME", "SALARY", "DATE", "PREVIOUS_SALARY", "TREND")
////      .orderBy("DATE_PARSED")
////
////    salaryTrend1.show()
//
//
////    ---------------------------------------------------------------------
////
////    val salaryData = Seq(
////      (1, "karthik", 1000),
////      (2, "moahn", 2000),
////      (3, "vinay", 1500),
////      (4, "Deva", 3000)
////    ).toDF("ID", "NAME", "SALARY")
////
////    val filteredData = salaryData.filter(col("SALARY") > 1500)
////
////    // Define window specification ordered by ID
////    val windowSpec = Window.orderBy("ID")
////
////    // Calculate lead and lag
////    val result = filteredData
////      .withColumn("SALARY_LAG", lag("SALARY", 1).over(windowSpec))
////      .withColumn("SALARY_LEAD", lead("SALARY", 1).over(windowSpec))
////
////    // Show the result
////    result.show()
////
////    // For better readability, you can also select specific columns
////    result.select("ID", "NAME", "SALARY", "SALARY_LAG", "SALARY_LEAD").show()
//
//  }
//}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//
////~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


//object scalaSpark_exam {
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder()
//      .appName("SalaryTrendAnalysis")
//      .master("local[*]")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    val ratingData = Seq(
//      ("User1", "Movie1", 4.5),
//      ("User1", "Movie2", 3.5),
//      ("User1", "Movie3", 2.5),
//      ("User1", "Movie4", 4.0),
//      ("User1", "Movie5", 3.0),
//      ("User1", "Movie6", 4.5),
//      ("User2", "Movie1", 3.0),
//      ("User2", "Movie2", 4.0),
//      ("User2", "Movie3", 4.5),
//      ("User2", "Movie4", 3.5),
//      ("User2", "Movie5", 4.0),
//      ("User2", "Movie6", 3.5)
//    ).toDF("User", "Movie", "Rating")
//
//    val window=Window.partitionBy(col("User")).orderBy(col("Rating")).rowsBetween(0,2)
//
//    val df2=ratingData.select(col("User"),col("Movie"),col("Rating"),avg(col("Rating")).over(window))
//
//    df2.show()
//
//  }
//}
//////~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

object scalaSpark_exam {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("complex_spark_app")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val orders = List(
      (1, "2023-03-01", "Delivered"),
      (2, "2023-04-10", "Pending"),
      (3, "2023-05-15", "Delivered"),
      (4, "2023-06-01", "Delivered")
    ).toDF("cust_id","order_date","del_status")

    val feedback = List(
      (1, "2023-03-05", Some(5)),
      (2, "2023-04-15", None),
      (3, "2023-05-20", Some(4)),
      (4, "2023-06-05",Some(3))
    ).toDF("cust_id","fb_date","fb_rating")

//    Perform a JOIN between customer orders and their feedback.
//    Validate that feedback ratings are submitted only for orders that have been delivered
//    (based on date comparison). Group by customer and calculate the average feedback score.
//    Use window aggregation to rank customers by feedback score, handling null feedback values.
//    Write the result to ORC format.

    val condition = orders("cust_id")===feedback("cust_id") and orders("del_status")==="Delivered"

    val joinType = "inner"

    val jd = orders.join(feedback,condition,joinType)
      .drop(feedback("cust_id"))
      //      .show()
      .withColumn("rnk",rank().over(Window.orderBy(col("fb_rating").desc)))
      .show()
//      .groupBy("cust_id").agg(avg(col("fb_rating")).desc).show()

  }
}