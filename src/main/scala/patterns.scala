object patterns {
  def main(args: Array[String]): Unit = {

//    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    println()
    println("Pattern 1 : ")
    println("------------")
//    1
//    2 2
//    3 3 3
//    4 4 4 4
//    5 5 5 5 5
    for(i<-1 to 5 by 1)
      {
        for(j<-1 to i by 1)
          {
            print(i+" ")
          }
        println()
      }

    //    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    println()
    println("Pattern 2 : ")
    println("------------")
//    1
//    1 2
//    1 2 3
//    1 2 3 4
//    1 2 3 4 5
    for(i<-1 to 5 by 1)
      {
        for(j<-1 to i)
          {
            print(j+" ")
          }
          println()
      }

    //    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    println()
    println("Pattern 3 : ")
    println("------------")
//    1 2 3 4 5
//    2 3 4 5
//    3 4 5
//    4 5
//    5
    for(i<-1 to 5 by 1)
      {
        for(j<-i to 5 by 1)
          {
            print(j+" ")
          }
          println()
      }

    //    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    println()
    println("Pattern 4 : ")
    println("------------")
//    1
//    2 3
//    4 5 6
//    7 8 9 10
//    11 12 13 14 15
    var n=1
    for(i<-1 to 5 by 1)
    {
      for(j<-1 to i)
      {
        print(n+" ")
        n=n+1
      }
      println()
    }
  }
}
