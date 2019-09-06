package com.cg

import java.text.SimpleDateFormat

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author SeraphimLucifar
 * @date 2019-09-05
 */
object KafkaToRedis extends App {
  val cfg=new SparkConf().setMaster("local[2]").setAppName("cg")
  val ssc=new StreamingContext(cfg,Seconds(5))
  ssc.sparkContext.setLogLevel("WARN")

  val kafkaParam=Map("bootstrap.servers"->"cg01:9092,cg02:9092,cg03:9092")
  val topics=Set("cg")
  val dStream=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParam,topics)
  dStream.flatMap(_._2.split(" ")).filter(!_.isEmpty).map((_,1)).reduceByKey(_+_).foreachRDD(rdd=>{
    rdd.foreachPartition(f=>{
      val fd=new SimpleDateFormat("yyyyMMdd_HH_mm")
      val hkey=fd.format(new java.util.Date())
      val jedis=JedisUtil.open
      jedis.select(1)
      f.foreach(x=>{
        val hfield=x._1
        if(jedis.hexists(hkey,hfield)){
          jedis.hincrBy(hkey,hfield,x._2)
          println(s"累加:${hkey}:${hfield}:${x._2}")
        }else{
          jedis.hset(hkey,hfield,x._2.toString)
          println(s"创建:${hkey}:${hfield}:${x._2}")
        }
      })
      JedisUtil.close(jedis)
    })
  })

  ssc.start()
  ssc.awaitTermination()



}
