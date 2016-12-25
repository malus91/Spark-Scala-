val input = sc.textFile("hdfs://cshadoop1/socNetData/networkdata/soc-LiveJournal1Adj.txt")
val userData = sc.textFile("hdfs://cshadoop1/socNetData/userdata/userdata.txt")
val userA = readLine("Enter first User : ")
val userB = readLine("Enter second User : ")
val userAFriends = input.filter(line => line.startsWith(userA + "\t")).flatMap(line => line.split("\t")(1).split(","))
val userBFriends = input.filter(line => line.startsWith(userB + "\t")).flatMap(line => line.split("\t")(1).split(","))
val unionOfFriends = userAFriends.union(userBFriends)
val mutualFriends = unionOfFriends.map(friend => (friend, 1)).reduceByKey(_ + _).filter(friendCount => (friendCount._2 == 2)).map(friend => (friend._1, friend._1))

val usersInfo = userData.map(user => user.split(",")).map(userInfo => (userInfo(0), userInfo(1) + ":" + userInfo(9)))
val friendsInfo = mutualFriends.join(usersInfo)
val output = friendsInfo.map(info => (userA + "\t" + userB, info._2._2)).reduceByKey(_ + ", " + _).map(info => (info._1 + "\t[" + info._2 + "]"))

output.collect.foreach(println)

