package com.cg

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author SeraphimLucifar
 * @date 2019-09-06
 */
object KafkaForCheckpiont extends App {
  val conf=new SparkConf().setAppName(getClass.getName).setMaster("local[2]")
  val ssc=new StreamingContext(conf,Seconds(5))
  ssc.sparkContext.setLogLevel("WARN")
  ssc.checkpoint("hdfs://cg01:8020/kafka/checkpoint")


  val kafkaParam=Map("bootstrap.servers"->"cg01:9092,cg02:9092,cg03:9092")
  val topic=Set("cg")
  val dStream=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParam,topic)
  dStream.flatMap(_._2.split(" ")).filter(!_.isEmpty).map((_,1)).updateStateByKey((values:Seq[Int],op:Option[Int])=>{
    var result=0
    if(op.isDefined){
    result=op.get
    }
    values.foreach(f=>{
      result+=f
    })
    Some(result)
  }).print()


  ssc.start()
  ssc.awaitTermination()



}
