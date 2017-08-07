package KafkaInt

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._

/**
  * Created by Sarang on 07-08-2017.
  * spark-submit --class KafkaInt.SparkStreamingWordCount --master yarn sparkstream_2.10-1.0.jar
  * nc -lk 15151
  */
object SparkStreamingWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Testing streaming").setMaster("yarn-client");
    val ssc = new StreamingContext(conf, Seconds(10));

    val lines = ssc.socketTextStream("gw01.it.com", 15151)
    val linesFlatMap = lines.flatMap(rec => rec.split(" "));
    val linesMap = linesFlatMap.map((_, 1));
    val lineRBK = linesMap.reduceByKey(_+_);
    lineRBK.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
