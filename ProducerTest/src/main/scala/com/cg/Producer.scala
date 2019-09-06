package com.cg

import java.util
import java.util.{Properties, TimerTask}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


/**
 * @author SeraphimLucifar
 * @date 2019-09-04
 */

object Test extends App{
  val list=List("hadoop","hello","azkaban","zhangsan","lemon","tree","zookeeper","service","李四")
  val random=new java.util.Random()
  val timer=new java.util.Timer()
  timer.schedule(new TimerTask {
    var index=0
    override def run(): Unit ={
      val msg=new StringBuilder
      for (i <- 1 to random.nextInt(20)){
        val num=random.nextInt(list.length)
        msg.append(list(num)).append(" ")
      }
      index+=1
      var id=index.toString
      kafkaProducer.sendmessage("cg",id,msg.toString())
      println(s"生产者:${msg}")
    }
  },0,1000)
//  val msg="消息111"
//  kafkaProducer.sendmessage("d1903","date",msg)
//  println(msg)

}


object kafkaProducer {
  def sendmessage(topic:String,key:String,msg:String): Unit = {
    val brokers = "cg01:9092,cg02:9092,cg03:9092"
    val props = new Properties()
    props.put("delete.topic.enable", "true")
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
//    props.put("partitioner.class", classOf[HashPartitioner].getName)
    props.put("queue.buffering.max.messages", "1000000")
    props.put("queue.enqueue.timeout.ms", "20000000")
    props.put("batch.num.messages", "1")
    props.put("producer.type", "sync")
//    val config = new ProducerConfig(props)
    val producer = new KafkaProducer[String, String](props)//key和value都是String类型
//     Thread.sleep(1000)
    val message=new ProducerRecord(topic,key,msg)
//    val message = new KeyedMessage[String, String](topic, key,presend.toString)
    producer.send(message)  //发送到指定的topic
  }

  def sendmessage(topic:String,msg:String): Unit = {
    val brokers = "cg01:9092,cg02:9092,cg03:9092"
    val props = new Properties()
    props.put("delete.topic.enable", "true")
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", classOf[StringSerializer])
    props.put("value.serializer", classOf[StringSerializer])
    //    props.put("partitioner.class", classOf[HashPartitioner].getName)
    props.put("queue.buffering.max.messages", "1000000")
    props.put("queue.enqueue.timeout.ms", "20000000")
    props.put("batch.num.messages", "1")
    props.put("producer.type", "sync")
    //    val config = new ProducerConfig(props)
    val producer = new KafkaProducer[String, String](props)//key和value都是String类型
    //     Thread.sleep(1000)
    val message=new ProducerRecord[String,String](topic,msg)
    //    val message = new KeyedMessage[String, String](topic, key,presend.toString)
    producer.send(message)  //发送到指定的topic
  }
}

