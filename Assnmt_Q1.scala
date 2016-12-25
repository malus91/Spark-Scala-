val input = sc.textFile("hdfs://cshadoop1/socNetData/networkdata/soc-LiveJournal1Adj.txt")
def buildFriendPair(user1:String,user2:String):(String,String)={
  if(user1.toInt>user2.toInt){
    return (user2,user1)
  }
  else{
    return (user1,user2)
  }
}
​
val userFriendList = input.map(friendList=>friendList.split("\\t")).
                     filter(friend=>(friend.size==2))
val userPairList = userFriendList.map(x=>(x(0),x(1).split(","))).flatMap(x=>x._2.map(z=>(buildFriendPair(x._1,z),x._2)))
val userPairMutual = userPairList.reduceByKey((a,b)=>a.intersect(b)).sortByKey()
val output = userPairMutual.collect.map(x=>x._1+"\t"+x._2.mkString(",")).filter(x=>x.split("\t").size==2)
output.foreach(println)
