# Get max priced product from products using core spark
val products = sc.textFile("/user/sarangdp/data/retail_db/products").filter(rec => !rec.startsWith("685")).map(rec => rec.split(","))
#get top product now

val topProduct = products.reduce((rec1, rec2) => {
 if(rec1(4).toFloat >= rec2(4).toFloat) rec1 else rec2
})


#no of order by status
val orders = sc.textFile("/user/sarangdp/data/retail_db/orders").map(rec => (rec.split(",")(3), rec)).countByKey

#Number of orders by order date and order status
val orders = sc.textFile("/user/sarangdp/data/retail_db/orders")
              .map(rec => ((rec.split(",")(1),rec.split(",")(3)), 1))
              .reduceByKey((acc,value)=> acc+value)

#Total Revenue per day
 val orders = sc.textFile("/user/sarangdp/data/retail_db/orders").map(rec=> (rec.split(",")(0).toInt, rec.split(",")(1)))
 val orderItems = sc.textFile("/user/sarangdp/data/retail_db/order_items").map(rec=> (rec.split(",")(1).toInt, rec.split(",")(4).toFloat))
 orders.join(orderItems).map(rec => (rec._2._1,rec._2._2)).reduceByKey((acc,value) => acc+value).foreach(println)
 
