//***Using Load Function- Parquet*************/

 userPar = spark.read.format("parquet").load("/user/sarangdp/users.parquet");
 userPar.select("name","favorite_color").write.save("namesAndFavColors.parquet");
 
 //***Using Load CSV*************/

peopleCsv = spark.read.load("/user/sarangdp/people.csv",format="csv",sep=";",inferSchema="true", header="true");
peopleCsv.write.save("/user/sarangdp/peopleJson",format="json");//stored as a directory
