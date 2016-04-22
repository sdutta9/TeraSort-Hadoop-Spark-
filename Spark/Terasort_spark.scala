val sortFile = sc.textFile("hdfs://ec2-52-207-231-47.compute-1.amazonaws.com:9000/user/shouvik/input/input100gb")

val sortedobj = sortFile.flatMap(line=>line.split("\n")).map(word=>(word.substring(0,10),word.substring(10))).sortByKey()

sortedobj.saveAsTextFile("hdfs://ec2-52-207-231-47.compute-1.amazonaws.com:9000/user/shouvik/output")









