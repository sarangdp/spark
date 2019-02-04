from pyspark.sql import Row

sc = spark.sparkContext;
lines = sc.textFile("/user/sarangdp/people.txt");
parts = lines.map(lambda p:p.split(","));
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])));
peopleDf = spark.createDataFrame(people);
peopleDf.createOrReplaceTempView("people");
teenagers = spark.sql("select name from people where age >= 13 and age <= 19");

teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect();
for name in teenNames:
  print(name);


 
