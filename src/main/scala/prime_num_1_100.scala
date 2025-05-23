object prime_num_1_100 {
  def main(args: Array[String]): Unit = {
//    Find the prime numbers between 1 to 100

    for(i<-1 to 100)
      {
        var num=i
        var count=0

        for(j<-1 to num)
          {
            if(num%j==0)
              {
                count=count+1
              }
          }

        if(count==2)
        {
          println(num)
        }
      }
  }
}

