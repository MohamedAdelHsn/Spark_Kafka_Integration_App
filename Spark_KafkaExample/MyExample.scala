package org.sparkapp.mysparkapp

import org.apache.spark.{SparkConf , SparkContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{StreamingContext , Seconds }

object MyExample {
  
  
  def main(args:Array[String]) : Unit = 
  {
    
  val kafkaParams = Map[String, Object](
   "bootstrap.servers" -> "localhost:9092",
   "key.deserializer" -> classOf[StringDeserializer],
   "value.deserializer" -> classOf[StringDeserializer],
   "group.id" -> "myGroupId",
   "auto.offset.reset" -> "latest",
   "enable.auto.commit" -> (false: java.lang.Boolean)
   )
   
    val conf = new SparkConf().setAppName("spark-kafka-Integration-App").setMaster("local[2]")
    
    val ssc = new StreamingContext(conf , Seconds(10))
    
    val dstream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](Array("myFirstTopic"), kafkaParams)
       
    )
    
    val streamWordCount  = dstream.map(_.value())
     .flatMap(_.split(" "))
     .filter(!_.equals(""))
     .map(word => (word.toLowerCase() , 1))
     .reduceByKey(_+_)
    
     
     streamWordCount.print()
    
     
    ssc.start()  
    ssc.awaitTermination()
    
  }
  
  
}
