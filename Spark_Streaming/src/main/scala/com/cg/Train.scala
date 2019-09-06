package com.cg

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author SeraphimLucifar
 * @date 2019-09-06
 */
object Train extends App {
  val conf=new SparkConf().setAppName("1").setMaster("local[2]")
  val sc=new SparkContext(conf)
  sc.setLogLevel("WARN")
  //数据
  val rdd1=sc.parallelize(List("lemon","tree","zookeeper","service","李四"),1).map((_,1))
  //过滤名单
  val rdd2=sc.parallelize(List("zookeeper","service","李四"),1).map((_,true))
  rdd1.fullOuterJoin(rdd2).foreach(println)
  println("***************")
  rdd1.leftOuterJoin(rdd2).foreach(println)
//  val rdd3=rdd1.leftOuterJoin(rdd2)
//  rdd3.filter(f=>{
//    if(f._2._2 isEmpty){
//      true
//    }else{
//      false
//    }
//  }).map(f=>{
//    (f._1,f._2._1)
//  }).foreach(println)


}
