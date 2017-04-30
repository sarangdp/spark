# spark - Global sorting
# sortByKey - desc and then take top n records
val orders = sc.textFile("/user/sarangdp/data/retail_db/orders").map(rec => (rec.split(",")(0).toInt, rec)).sortByKey(false)
            .take(5).foreach(println)

# use top(n) which replace the -sortByKey and take(n)
val orders = sc.textFile("/user/sarangdp/data/retail_db/orders").map(rec => (rec.split(",")(0).toInt, rec)).top(5).foreach(println)

#using takeOrdered - Return the first n elements of the RDD using either their natural order or a custom comparator.Always sort in ascending
val orders = sc.textFile("/user/sarangdp/data/retail_db/orders").map(rec => (rec.split(",")(0).toInt, rec)).takeOrdered(5).foreach(println)

#using ordering- in descending order
val orders = sc.textFile("/user/sarangdp/data/retail_db/orders").map(rec => (rec.split(",")(0).toInt, rec))
            .takeOrdered(5)(Ordering[Int].reverse.on(x => x._1)).foreach(println)
            
#Problem statement - Get data sorted by product price per category
val products = sc.textFile("/user/sarangdp/data/retail_db/products").filter(rec=> !rec.startsWith("685"))
val products = sc.textFile("/user/sarangdp/data/retail_db/products").filter(rec=> !rec.startsWith("685"))
                 .map(rec=> (rec.split(",")(1).toInt, rec))
val prodGroupBy = products.groupByKey()
prodGroupBy.map(rec => rec._2.toList.sortBy(k=> (k.split(",")(4).toFloat))).take(100).foreach(println)

#To get topN products by price in each category
def getTopNProducts(rec:(Int, Iterable[String]), topN:Int): Iterable[String] ={
   val x: List[String] = rec._2.toList.sortBy(k=> k.split(",")(4).toFloat).take(topN)
   return x
 }
 
 prodGroupBy.flatMap(rec => getTopNProducts(rec,3)).take(10).foreach(println)
            
#Using hive sql

#Global sorting and ranking
#Using order by is not efficient, it serializes
sqlContext.sql("select * from products order by product_category_id, product_price desc limit 10").foreach(println)

#Using distribute by sort by (to distribute sorting and scale it up)
sqlContext.sql("select * from products distribute by product_category_id sort by product_price desc").foreach(println)

sqlContext.sql("select * from 
(select p.*, dense_rank() over (partition by product_category_id order by product_price desc) dr 
from products p distribute by
 product_category_id) q where q.dr <3 order by q.product_category_id").take(10).foreach(println)

