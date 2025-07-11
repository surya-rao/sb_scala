import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class sparkSQL_exam1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("StudentDf")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    import org.apache.spark.sql.Row

    val studentSchema = StructType(Array(
      StructField("student_id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("score", IntegerType, true),
      StructField("subject", StringType, true),
    ))

    val rowData = Seq(
      Row(1, "Alice", 92, "Math"),
      Row(2, "Bob", 85, "Math"),
      Row(3, "Carol", 77, "Science"),
      Row(4, "Dave", 65, "Science"),
      Row(5, "Eve", 50, "Math"),
      Row(6, "Frank", 82, "Science")
    )

    val studentDf2 = spark.createDataFrame(spark.sparkContext.parallelize(rowData), studentSchema)

    studentDf2.createOrReplaceTempView("students")

    val result_df1 = spark.sql(
      """
    SELECT student_id, name, score, subject
    FROM students
    """)

    result_df1.createOrReplaceTempView("students")

    //  Case When
    val students_with_grade_sql = spark.sql(
      """
    SELECT student_id, name, score, subject,
           CASE
               WHEN score >= 90 THEN 'A'
               WHEN score >= 80 THEN 'B'
               WHEN score >= 70 THEN 'C'
               WHEN score >= 60 THEN 'D'
               ELSE 'F'
           END as grade
    FROM students
    """)

    students_with_grade_sql.show()
  }
}
