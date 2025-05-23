object loops {
  def main(args: Array[String]): Unit = {
    println("1 to 5 odd numbers (step by 1) using FOR loop")
    for(i <- 1 to 5 by 1)
      {
        println(i)
      }

    println("1 to 10 odd numbers (step by 2) using FOR loop")
    for(j <- 1 to 10 by 2)
    {
      println(j)
    }

    println("print 5 yo 1 using WHILE loop (step down by 1)")
    // variable declaration (assigning 5 to a)
    var a = 5

    // loop execution
    while (a > 0)
    {
      println("a is : " + a)
      a = a - 1;
    }

    println("print 1 to 5 using DO-WHILE loop (step by 1)")
    // variable declaration (assigning 1 to a)
    var b = 1;
    do
    {
      println("b is : " + b);
      b = b + 1;
    }
    while (b <= 5);
  }
}