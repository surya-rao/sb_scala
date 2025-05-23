object chapter1 {
  def main(args: Array[String]): Unit = {
//    Variables
    println("VAL examples -- Its Immutable")
    val a=10
    val b:Int=20
    val c:String="Surya"
    val d:Double = 3.14
    println(a)
    println(b)
    println(c)
    println(d)
    println("--------------")
    println("VAR examples -- Its Mutable")
    var m=100
    var n:Int=120
    m=150
    n=200

    val o:Float=3.14f

    println(m)
    println(n)
    println(o)
  }
}
