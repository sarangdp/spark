package exercise

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._


object RevPerDeptWithDF {
  def main(args: Array[String]): Unit = {
    val props = ConfigFactory.load()
    val conf = new SparkConf().
      setAppName("Rev Per Dept With DF")
      .setMaster(props.getConfig(args(0)).getString("executionMode"))
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val inputPath = args(1)
    val outputPath = args(2)

    val fs :FileSystem = FileSystem.get(sc.hadoopConfiguration)
    val op = new Path(outputPath)
    if(!fs.exists(new Path(inputPath))){
      println("Invalid input path")
    }

    if(fs.exists(op)){
      fs.delete(op, true)
    }

    val ordersDF = sc.textFile(inputPath + "/orders").map(rec => {
      val r = rec.split(",")
      Orders(r(0).toInt, r(1), r(2).toInt, r(3))
    }).toDF()

    val orderItemsDF = sc.textFile(inputPath+"/order_items").
      map(rec => {
        val a = rec.split(",")
        OrderItems(
          a(0).toInt,
          a(1).toInt,
          a(2).toInt,
          a(3).toInt,
          a(4).toFloat,
          a(5).toFloat)
      }).toDF()

    val productsDF = sc.textFile(inputPath+"/products").filter(rec => !rec.startsWith("685")).map(rec => {
      val r = rec.split(",")
      Products(r(0).toInt, r(1).toInt, r(2), r(3), r(4).toFloat, r(5))
    }).toDF()

    val categoriesDF = sc.textFile(inputPath+"/categories").map(rec => {
      val r = rec.split(",")
      Categories(r(0).toInt, r(1).toInt, r(2))
    }).toDF()

    val deptDF = sc.textFile(inputPath+"/departments").map(rec => {
      val r = rec.split(",")
      Departments(r(0).toInt, r(1))
    }).toDF()

    ordersDF.filter(ordersDF("order_status") === "COMPLETE" || ordersDF("order_status") === "PENDING").join(orderItemsDF, ordersDF("order_id") === orderItemsDF(
      "order_item_order_id")).join(productsDF, orderItemsDF("order_item_product_id")===productsDF("product_id"))
      .join(categoriesDF, productsDF("product_category_id")===categoriesDF("category_id")).join(deptDF, categoriesDF("category_department_id")===deptDF("department_id")).groupBy(deptDF("department_name"))
      .agg(sum(orderItemsDF("order_item_subtotal"))).sort(deptDF("department_name")).rdd.saveAsTextFile(outputPath)

  }
}
