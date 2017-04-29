# Using Spark core API

# Problem statement, get the revenue orders from order_items on daily basis
val orders = sc.textFile("/user/sarangdp/data/retail_db/orders").map(rec => (rec.split(",")(0).toInt, rec.split(",")(1)))
val ordersItems = sc.textFile("/user/sarangdp/data/retail_db/order_items").map(rec => (rec.split(",")(1).toInt, rec.split(",")(4).toFloat))

val orderJoin = orders.join(ordersItems)
val groupOrders = orderJoin.map(rec => (rec._2._1, rec._2._2)).reduceByKey((acc, value)=> acc+value)

# Problem statement, get the number of orders from order_items on daily basis
val orderJoin = orders.join(ordersItems)
val ordersCount = orderJoin.map(rec => (rec._2._1+","+rec._1)).distinct().map(rec => (rec.split(",")(0), 1))
                    .reduceByKey((acc,value) => acc+value)
                    
# Joining order count per day and revenue per day
val finalJoin = groupOrders.join(ordersCount)

#using hive context- sqlContext
val orderTotal = sqlContext.sql("select o.order_date,count(distinct o.order_id) order_count,round(sum(oi.order_item_subtotal),2) order_total from orders o j
oin order_items oi on o.order_id=oi.order_item_order_id group by o.order_date")

#using SQL native context
case class Orders(
   order_id:Int,
   order_date:String,
   order_customer_id:Int,
   order_status:String)
   
   case class OrderItems(
        order_item_id: Int,
        order_item_order_id: Int,
        order_item_product_id: Int,
        order_item_quantity: Int,
        order_item_subtotal: Float,
        order_item_price: Float)
        
  val sqlc = new org.apache.spark.sql.SQLContext(sc)  
  import sqlc.implicits._
  
  val orders = sc.textFile("/user/sarangdp/data/retail_db/orders").map(rec => rec.split(","))
  val ordersItems = sc.textFile("/user/sarangdp/data/retail_db/order_items").map(rec => rec.split(","))
