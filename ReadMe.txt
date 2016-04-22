TeraSort_Hadoop_and_Spark

----------------------------------------------------------
Hadoop
----------------------------------------------------------
Configure and launch hadoop on C3.Large instance as mentioned in Prog2_Report.

hadoop fs -mkdir /user/shouvik/input

hadoop fs -put /home/ubuntu/sharedMemory/64/inputfile /user/shouvik/input/
hadoop fs -ls /user/shouvik/input/


hadoop com.sun.tools.javac.Main TeraSort_hadoop.java
jar -cf ts.jar TeraSort_hadoop*.class
hadoop jar ts.jar TeraSort_hadoop /user/shouvik/input /user/shouvik/output


hadoop fs -ls /user/shouvik/output/
hadoop fs -get /user/shouvik/output/part-r-00000 /home/ubuntu
./valsort /home/ubuntu/part-r-00000


----------------------------------------------------------
Spark
----------------------------------------------------------

Configure and launch spark on C3.Large instance as mentioned in Prog2_Report.

cd spark/bin

./spark-shell



val sortFile = sc.textFile("hdfs://ec2-52-207-231-47.compute-1.amazonaws.com:9000/user/shouvik/input/input100gb")





val sortedobj = sortFile.flatMap(line=>line.split("\n")).map(word=>(word.substring(0,10),word.substring(10))).sortByKey()



sortedobj.saveAsTextFile("hdfs://ec2-52-207-231-47.compute-1.amazonaws.com:9000/user/shouvik/output")









------------------------------------------------------------