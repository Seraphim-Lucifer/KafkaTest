package com.cg

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * @author SeraphimLucifar
 * @date 2019-09-05
 */
object Test extends App {
  var groupid = "cg_test"
  var consumerid = "cg"
  var topic = "cg"
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
  val consumer = new KafkaConsumer[String,String](props)
  consumer.subscribe(java.util.Arrays.asList(topic))
  while(true){
    val tmp=consumer.poll(1000)
    val it=tmp.iterator()
    while(it.hasNext){
      val t=it.next()
      println(s"${t.key()}:${t.value()}")
    }

  }


}
