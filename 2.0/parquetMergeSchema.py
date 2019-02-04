from pyspark.sql import Row

//Create Square dataframe and write in 1 dir
sqDF = spark.createDataFrame(sc.parallelize(range(1,6)).map(lambda r:Row(single=r, double=r**2));
sqDF.write.parquet("/user/sarangdp/data/Key=1");

//Create triple dataframe and write in 2 dir
cubeDF= spark.createDataFrame(sc.parallelize(range(1,6)).map(lambda r:Row(single=r,triple = r**3)));
cubeDF.write.parquet(""/user/sarangdp/data/Key=2");

//Merge Schema by reading both dir
 mergeDF = spark.read.option("mergeSchema","true").parquet("/user/sarangdp/data");
 
 megeDF.show();
 
 /******************output*******************************/
 +------+------+------+---+
|double|single|triple|Key|
+------+------+------+---+
|     9|     3|  null|  1|
|    16|     4|  null|  1|
|    25|     5|  null|  1|
|  null|     3|    27|  2|
|  null|     4|    64|  2|
|  null|     5|   125|  2|
|     1|     1|  null|  1|
|     4|     2|  null|  1|
|  null|     1|     1|  2|
|  null|     2|     8|  2|
+------+------+------+---+
