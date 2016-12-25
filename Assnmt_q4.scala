val input = sc.textFile("hdfs://cshadoop1/socNetData/networkdata/soc-LiveJournal1Adj.txt")
val userData = sc.textFile("hdfs://cshadoop1/socNetData/userdata/userdata.txt")
def calcAge(dob:String):Int={
  val age = 2016-dob.split("/")(2).toInt
  return age
}

val frndList   = input.map(x=>x.split("\t")).filter(x=>x.size==2)
val userAgeMap = userData.map(y=>y.split(",")).filter(y=>y.size==10).map(y=>(y(0),calcAge(y(9))))
val userFndMap = frndList.map(x=>(x(0),x(1).split(","))).flatMap(x=>x._2.map(z=>(z,x._1)))
val userFrndDetails = userFndMap.join(userAgeMap).map(x=>(x._2._1,x._2._2))
val userSort = userFrndDetails.reduceByKey((a,b)=>if(a>b) a else b)
val outputFormat = userData.map(z=>z.split(",")).filter(z=>z.size==10).map(z=>(z(0),z(3)+","+z(4)+","+z(5)))
val output = userSort.join(outputFormat).map(z=>(z._2._1,(z._2._2,z._1)))
val result = output.sortByKey(false).map(z=>z._2._2+", "+z._2._1+","+z._1).take(20).foreach(println)
