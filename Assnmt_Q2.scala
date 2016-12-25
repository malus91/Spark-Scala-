val input = sc.textFile("hdfs://cshadoop1/socNetData/networkdata/soc-LiveJournal1Adj.txt")
val user1 = readLine("Enter first User : ")
val user2 = readLine("Enter second User : ")
val userData =input.map(user=>user.split("\\t"))
 
val u1frnds = userData.filter(user=>(user1==user(0).toInt)).filter(user=>(user.size==2)).flatMap(user=>user(1).split(","))
val u2frnds = userData.filter(user=>(user2==user(0).toInt)).filter(user=>(user.size==2)).flatMap(user=>user(1).split(","))

val u1u2Mutual =u1frnds.intersection(u2frnds).collect()
val mutualFriends = user1+","+user2+"\t"+u1u2Mutual.mkString(",")