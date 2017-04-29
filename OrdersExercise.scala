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
 
#average revenue per day
#parse orders and orderItems
 val orders = sc.textFile("/user/sarangdp/data/retail_db/orders").map(rec=> (rec.split(",")(0).toInt, rec.split(",")(1)))
 val orderItems = sc.textFile("/user/sarangdp/data/retail_db/order_items").map(rec=> (rec.split(",")(1).toInt, rec.split(",")(4).toFloat))
#join order and orderItems
val orderMap = orders.join(orderItems).map(rec=> (rec._2._1, rec._2._2))
#using aggregateByKey
val orderAgr = orderMap.aggregateByKey((0.0,0))((seqAgr, rev)=> {((seqAgr._1 + rev),(seqAgr._2+1))},
                                                 (combAgr, combVal) => {((combAgr._1+ combVal._1),(combAgr._2+combVal._2))}
                                               )
val avgRevPerDay = orderAgr.map(rec => (rec._1, rec._2._1/rec._2._2))
 avgRevPerDay.foreach(println)

#Customer id with max revenue
val customers = sc.textFile("/user/sarangdp/data/retail_db/customers").map(rec => (rec.split(",")(0).toInt, (rec.split(",")(1), rec.split(",")(2))))
val orders = sc.textFile("/user/sarangdp/data/retail_db/orders").map(rec=> (rec.split(",")(2).toInt, rec.split(",")(0).toInt))
val custOrders = customers.join(orders).map(rec => (rec._2._2, rec._2._1))
val orderItems = sc.textFile("/user/sarangdp/data/retail_db/order_items").map(rec => (rec.split(",")(1).toInt, rec.split(",")(4).toFloat))
val custTotal = custOrders.join(orderItems).map(rec => (rec._2._1, rec._2._2)).reduceByKey((acc,value) => acc+value)
val topCust = custTotal.reduce((rec1, rec2) => {
 if (rec1._2 >= rec2._2) rec1 else rec2
})

#Using hive query
 select c.customer_id,o.order_date,sum(oi.order_item_subtotal)/count(oi.order_item_id) 
from customers c join orders o on c.customer_id = o.order_customer_id join order_items oi on o.order_id =oi.order_item_order_id 
group by  c.customer_id,o.order_date
