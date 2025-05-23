object arr_element_cnt_dups_uniqs {
  def main(args: Array[String]): Unit = {
    var arr = Array(10, 10, 10, 20, 20, 30, 30, 30, 40, 50, 60, 70, 80, 90)

    for(i<-0 until arr.length)
      {
        var e=1
        var str1: String = ""
        var str2: String = ""
        for(j<-i+1 until arr.length)
          {
            if(arr(i)==arr(j)){
              e=e+1
            }
            if(str1=="")
              {str1=arr(i)+":"+e}
            else
              {str2=arr(i)+":"+e}

            if(str1==str2)
              {str2=arr(i)+":"+e}

//            if


//            println(str)
          }

      }
  }
}
