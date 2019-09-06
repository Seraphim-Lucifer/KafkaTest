package com.cg

import java.util

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author SeraphimLucifar
 * @date 2019-09-06
 */
object WindowTest extends App {
  val conf=new SparkConf().setMaster("local[2]").setAppName("window")
  val ssc=new StreamingContext(conf,Seconds(5))
  val kafkaParam=Map("bootstrap.servers"->"cg01:9092,cg02:9092,cg03:9092")
  val topic=Set("cg")
  val dStream=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParam,topic)
  dStream.flatMap(_._2.split(" ")).filter(!_.isEmpty).map((_,1)).reduceByKeyAndWindow((a:Int,b:Int)=>{
    (b-a)
  },Seconds(10),Seconds(5)).print
  ssc.start()
  ssc.awaitTermination()


}
