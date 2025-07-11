object scala_test {
  def main(args: Array[String]): Unit = {

////    Print the elements in the odd indexes (index starts at 0) -- Start
//    val arr=Array(10,20,30,40,50,60,70,80,90)
//
//    for(i<-arr.indices)
//      {
//        if(i%2!=0){println(arr(i))}
//      }
//    ----------------------------------------
////    Fibonacci Series in Scala
//    import scala.collection.mutable.ArrayBuffer
//
//    val fib = ArrayBuffer(0, 1)
//    val n=5
//    for(i<- 2 until n) {
//      val num:Int = fib(fib.length-1)+fib(fib.length-2)
//      fib += num
////      n+=1
//    }
//    println("Fibonacci series: " + fib.mkString(", "))

//    // Class example1
//def sum(a:Int,b:Int): Int={
//  var c=a+b
//  c    // converts Unit to the return type of Int.
//}
//
//    val a=sum(10,20)
//    print(a)

//    // Class Example 2
//    def doubler(a:Int): Int = {
//      var c=a*2
//      c
//    }
//
//    def func(x:Int,f:Int=>Int):Int={
//      f(x)
//    }
//    print(func(10,doubler))

//    ----------------------------------------
////    Class Example 3
//    val arr=Array(10,20,30,40,50)
//
//    val result=arr.map(x=>x*2)
//
//    for(i<-0 until result.length){
//      print(result(i))
//    }

//    ----------------------------------------
//    //Class Example 4 --> Pure and Impure Functions
//    def result(arr:Array[Int]) ={
//
//      for(i<-0 until arr.length){
//        println(arr(i))
//      }
//
//      arr(1)=100
//
//
//      println("after channge")
//      for(i<-0 until arr.length){
//        println(arr(i))
//      }
//
//    }
//
//    result(Array(10,20,30,40,50,60,70))

//    ----------------------------------------
//    def result(a:Int) ={
////      a=100   throws error as a is by default considered as val.
//      var c=a*2
//      c
//    }
//    print(result(10))
  //    ----------------------------------------

    import org.apache.spark.SparkContext

//    val sc = new SparkContext("local[2]","Use_parallelize_example")
//    val mylist = Array(10, 20, 30, 40, 50, 60, 70, 80, 90, 100)
//    val rdd1 = sc.parallelize(mylist)


//    ~~~~~~~~~~~~~~~~~~~~~~~~~~~Mean and AVG ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
////    println(rdd1.mean())
//
//    val sum1 = rdd1.reduce((x,y)=>x+y)
////    val avg = sum1/(mylist.length)
////    val cnt = rdd1.count().toDouble
//    val cnt = rdd1.count()
//
//    println("Sum is : ", sum1)
//    println("count of elements : ", cnt)
//    println("Average is : ", sum1/cnt)
//    println("Mean is : ", rdd1.mean())
    //    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

//    // Filter() function
//
//    val filteredRdd = rdd1.filter(x => x % 20 == 0)
//    filteredRdd.collect.foreach(println)
    //    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


  }
}
