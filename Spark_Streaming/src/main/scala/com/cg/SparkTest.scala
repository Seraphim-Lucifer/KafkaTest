package com.cg

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.kafka010.KafkaUtils

/**
 * @author SeraphimLucifar
 * @date 2019-09-05
 */
object SparkTest extends App {
  val cfg=new SparkConf().setAppName(getClass.getName).setMaster("local[2]")
  val ssc=new StreamingContext(cfg,Seconds(5))
  val dStream=ssc.socketTextStream("cg01",11111)
  dStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print

  ssc.start()
  ssc.awaitTermination()

}
