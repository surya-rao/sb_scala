import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, current_date, dense_rank, lag, lead, lower, months_between, rank, round, sequence, split, when}


//object Spark_DataFrames_1 {
//  def main(args: Array[String]): Unit = {
//
//
////  //    Approach 1
////    val spark = SparkSession.builder()
////      .appName("first_spark_df")
////      .master("local[*]")
////      .getOrCreate()
////
////    val df = spark.read.format("csv")
////      .option("header",true)
////      .option("path","C:/Users/surya/OneDrive/Desktop/details.csv")
////      .load()
////
////    df.show(5,false)
//  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
////    val sparkconf=new SparkConf()
////        sparkconf.set("spark.app.name","spark-program")
////        sparkconf.set("spark.master","local[*]")
////
////
////    val spark=SparkSession.builder()
////      .config(sparkconf)
////      .getOrCreate()
////
////    val df=spark.read.option("header",true).csv("C:/Users/surya/OneDrive/Desktop/details_2.csv")
////
////    df.show(5,false)
////
////    df.write
////      .format("csv")
////      .option("header",true)
////      .option("path","C:/Users/surya/OneDrive/Desktop/details_2_write")
////      .save()
//
//    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//
//    // When & Otherwise
//
//    val conf=new SparkConf()
//    conf.set("spark.app.name","spark-program")
//    conf.set("spark.master","local[4]")
//    //    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//    //    conf.set("spark.kryo.registrator","myKryo")
//    //    conf.set("spark.kryoserializer.buffer","1024k")
//
//    val spark=SparkSession.builder()
//      .config(conf)
//      .getOrCreate()
//
//    import spark.implicits._
//
//    val df1=spark.read
//      .format("csv")
//      .option("header","true")
//      .option("path","C:/Users/surya/OneDrive/Desktop/details.csv")
//      .load()
//
////    val df2=df1.select(
////      col("id"),
////      col("name"),
////      col("salary"),
////      when(col("salary")>5000,"rich").
////        otherwise("poor").alias("status")
////    )
//
//    df1.createOrReplaceTempView("surya")
//
////    spark.sql(
////      """
////        select
////         id,
////         name,
////         salary,
////         case
////         when salary>5000 then "rich"
////         else "poor"
////        end as status
////        from  surya
////        """
////    ).show()
//  }
//}

object ScalaSpark_DF {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    conf.set("spark.app.name","spark-app1")
    conf.set("spark.master","local[2]")

    val data_dir = "C:/Users/surya/OneDrive/Documents/Surya/Source_Code/SB_Projects/sb_scala/scala_2-12-0_sbt_1-0_project/src/main/data/"

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    //region  Read details.csv

//    val input_data = {data_dir}+"details.csv"
//
//    val df1 = spark.read
//      .format("csv")
//      .option("header","true")
//      .option("path",input_data)
//      .load()
//
//    df1.select(
//      col("id"),
//      col("name"),
//      col("salary"),
//      when(col("salary")>5000, "Rich").otherwise("Poor").alias("Status")
//    ).show()

    //end region

    //region --> DataFrames Labs - Load Data (Common for all the 15 questions from COMBBINATIONAL_QUESTIONS)
    val data = List(
      ("Karthik", "karthik@gmail.com", "2023-12-12", "Data Engineer", 85000),
      ("Veer", "veer@yahoo.com", "2022-11-01", "Business Analyst", 65000),
      ("Veena", "veena@company.org", "2021-06-15", "Data Scientist", 105000),
      ("Vinay", "vinay@gmail.com", "2024-01-10", "Intern", 25000),
      ("Vijay", "vijay@hotmail.com", "2020-05-22", "Senior Manager", 125000)
    )

    val df = data.toDF("name", "email", "join_date", "designation", "salary")
//    df.show()
    //endregion

    //region Question 1: Performance Banding, Experience Category, and Domain Detection
    //
    //Task:
    //
    //From the employee data, do the following:
    //	 Derive email_domain by extracting everything after @ and converting it to lowercase.
    //	 Create a new column experience_years using months_between and round it to nearest integer.
    //	 Based on experience:
    //		o < 1 year → "New"
    //		o 1 to <3 → "Experienced"
    //		o >=3 → "Veteran"
    //	 Create a new column salary_band:
    //		o < 50000 → "Low"
    //		o 50000 to 100000 → "Medium"
    //		o > 100000 → "High"
    //	 Mark is_internal as "Yes" if email ends with company.org, else "No".
    //	 Rename salary to current_salary.

    //endregion

    //region --> Question 1 - Answer
    val df2 = df.select(
      lower(split(col("email"),"@")(1)).as("email_domain"),

      current_date(),col("join_date"),

      round(months_between(current_date,col("join_date")),0).as("experience_years"),

      when((col("salary")) < 50000,"Low")
        .when((col("salary") > 50000 && col("salary") < 100000),"Medium")
        .otherwise("High").as("salary_band"),

      when(col("email").endsWith("company.org"),"Yes")
        .otherwise("No").as("Is_Internal_Emp?"),

      col("salary").as("current_salary")
    )

    df2.withColumn("experience_status",
              when((col("experience_years")/12)<1,"New")
                .when(((col("experience_years")/12)>1 && (col("experience_years")/12)<3),"Experienced")
                .otherwise("veteran")
    ).show()

    //endregion



    spark.stop()

  }
}
