import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, when}


object SQL_Spark_DF {
  def main(args: Array[String]): Unit = {

    val data_dir = "C:/Users/surya/OneDrive/Documents/Surya/Source_Code/SB_Projects/sb_scala/scala_2-12-0_sbt_1-0_project/src/main/data/"

    //region Create SparkSession using SparkConf
    // endregion
    val conf = new SparkConf()
    conf.set("spark.app.name","myApp")
    conf.set("spark.master","local[2]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    //region Load CSV data to dataframe
    //endregion
    val input_data = {data_dir}+"details.csv"

    val df1 = spark.read
      .format("csv")
      .option("header","True")
      .option("path",input_data)
      .load()

    //region This Approach-1 is SCALA-SPARK (1st program - details.csv - Add new column "Rich" or "Poor")
    // endregion
//    val df2 = df1.select(
//      col("id"),
//      col("name"),
//      col("salary"),
//      when(col("salary")>5000,"Rich").otherwise("Poor").alias("Status")
//    )
//
//    df2.show()

    //region  This Approach-2 is SparkSQL (1st Program - details.csv - Add new column "Rich" or "Poor")
    // endregion
//    val df3 = df1.createOrReplaceTempView("emp_sal")
//
//    spark.sql(
//      """
//      SELECT  id,
//              name,
//              salary,
//              CASE
//               WHEN salary > 5000 THEN "Rich"
//               ELSE "Poor"
//               END as Status
//      FROM emp_sal
//      """).show()
//
//    spark.stop()

    //region Reading Data from a List or Array or Tuple -- Using (Option 1) toDF (imported from implicits)
    //endregion
    val employees = List(
      (1, "AJAY", 28),
      (2, "VIJAY", 35),
      (3, "MANOJ", 22)
    ).toDF("id", "name", "age")

//    employees.show()

    //region - Reading Data from a List or Array or Tuple -- Using (Option 2) createDataFrame(list of tuples,schema)
    //endregion
//    val employees2 = List(
//      (1, "AJAY", 28),
//      (2, "VIJAY", 35),
//      (3, "MANOJ", 22)
//    )
//
//    val df = spark.createDataFrame(employees2)
//    val inputDF = df.toDF("id","name","age")
//
//    inputDF.show()

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

//region -- Question: How would you add a new column is_adult which is true if the age is
    // greater than or equal to 25, and false otherwise?
    //endregion

    //region --  Scala-Spark --> Start
    //endregion
//    employees.select(
//      col("id"),
//      col("name"),
//      col("age"),
//      when(col("age")>=25, "True").otherwise("False").alias("Is_Adult")
//    ).show()
//    // Scala-Spark --> End

    //region -- SparkSQL --> Start
    //endregion
//      val emp_df = employees.createOrReplaceTempView("empDF_table")
//
//    spark.sql(
//      """
//        Select id, name,age, case when age>=25 then "True" else "False" end as Is_Adult
//        From empDF_table
//      """
//    ).show()
//    //    // SparkSQL --> End




    spark.stop()
  }
}
