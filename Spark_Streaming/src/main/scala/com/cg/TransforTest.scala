package com.cg

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author SeraphimLucifar
 * @date 2019-09-06
 */
object TransforTest extends App {
  val conf=new SparkConf().setAppName("transfortest").setMaster("local[2]")
  val ssc=new StreamingContext(conf,Seconds(5))
  ssc.sparkContext.setLogLevel("WARN")

  val frdd=ssc.sparkContext.parallelize(List("lemon","tree","zookeeper","service","李四")).map((_,true))
  val topic=Set("cg")
  val kafkaParam=Map("bootstrap.servers"->"cg01:9092,cg02:9092,cg03:9092")
  val dStream=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParam,topic)

  dStream.flatMap(_._2.split(" ")).filter(!_.isEmpty).map((_,1)).transform(rdd=>{
    val leftrdd=rdd.leftOuterJoin(frdd)
    val needwords=leftrdd.filter(t=>{
      val x=t._1
      val y=t._2
      if(y._2.isEmpty){
        true
      }else{
        false
      }
    })
    needwords.map(f=>{
      (f._1,f._2._1)
    })
  }).reduceByKey(_+_).print()
//  needwordDS.reduceByKey(_+_).print()


  ssc.start()
  ssc.awaitTermination()

}
