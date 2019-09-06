package com.cg

import java.util.Properties

import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig}
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * @author SeraphimLucifar
 * @date 2019-09-04
 */
object kafka_consumer extends App{
  var groupid = "cg_test"
  var consumerid = "cg"
  var topic = "d1903"
  val props:Properties = new java.util.Properties()
  props.put("bootstrap.servers", "cg01:9092,cg02:9092,cg03:9092")
  props.put("group.id", groupid)
  props.put("client.id", "test")
  props.put("consumer.id", consumerid)
  props.put("auto.offset.reset", "earliest")
  props.put("key.deserializer", classOf[StringDeserializer])
  props.put("value.deserializer", classOf[StringDeserializer])
  props.put("auto.commit.enable", "true")
  props.put("auto.commit.interval.ms", "100")
//  val consumerConfig:ConsumerConfig = new ConsumerConfig(props)
  val consumer = new KafkaConsumer[String,String](props)
//  val consumer = Consumer.create(consumerConfig)
    consumer.subscribe(java.util.Arrays.asList(topic))
    while(true){
      val tmp=consumer.poll(1000)
      val it=tmp.iterator()
      while(it.hasNext){
        val t=it.next()
        println(s"${t.key()}:${t.value()}")
      }

    }


//  val topicCountMap = Map(topic -> 1)
//  val consumerMap = consumer.createMessageStreams(topicCountMap)
//  val streams = consumerMap.get(topic).get //从topic中获取源数据
//  for (stream <- streams) {
//    val it = stream.iterator()
//    while (it.hasNext()) {
//      val messageAndMetadata = it.next()
//      //val message = s"Topic:${messageAndMetadata.topic}, PartitionID:${messageAndMetadata.partition}, " +
//      // s"Offset:${messageAndMetadata.offset},Message Payload: ${new String(messageAndMetadata.message())}"
//       val message:String = s"${new String(messageAndMetadata.message())}"
//      val key:String = s"${messageAndMetadata.partition}".toString
//      var presend :String = ""
//      try{
//        val content_text: String = getContent(message)
//        if (content_text != "Invalid Map") {
//          val result = getScore(content_text)
//          presend = message.substring(0,message.length - 1) + "," + result.substring(1)
//        }
//      }catch {
//        case _ => presend = message
//      }
//      sendmessage(presend,"oyp-test-No1",key)
//    }
//  }
//}
}