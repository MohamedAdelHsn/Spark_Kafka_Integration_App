import org.apache.spark.{SparkConf , SparkContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{StreamingContext , Seconds }
import org.apache.hadoop.hbase.{HBaseConfiguration , HTableDescriptor , TableName }
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/*
   
   Here is a simple big data streaming pipeline using integration with 
   [Apache Kafka  <-- Producer  ,  Apache Spark Streaming <-- Consumer  , Apache Hbase <-- Data Storage ]

   @Author : Mohamed Adel Hassan 
 
*/


object StreamingData {
  
  def main(args :Array[String]):Unit = {
  
  
  val kafkaParams = Map[String, Object](
   "bootstrap.servers" -> "localhost:9092",
   "key.deserializer" -> classOf[StringDeserializer],
   "value.deserializer" -> classOf[StringDeserializer],
   "group.id" -> "myGroupId",
   "auto.offset.reset" -> "earliest",
   "enable.auto.commit" -> (false: java.lang.Boolean)
   )
   
    val conf = new SparkConf().setAppName("spark-kafka-Integration-App").setMaster("local[2]")
    
    val ssc = new StreamingContext(conf , Seconds(10))
    
  
    // ###################################### EXTRACT DATA FROM APACHE KAFKA  ######################################## //
  
    val dstream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](Array("myFirstTopic"), kafkaParams)
       
    )
  
    def updateFunc(values:Seq[Int] , state:Option[Int]) :Some[Int]= 
    {
    
     val current = values.foldLeft(0)(_+_)
     val prev = state.getOrElse(0)
     
     Some(current + prev)
     
    
    }
    
    
   // ###################################### TRANSFORM DATA USING SPARK STREAMING  ##################################################### //
  

    val streamingWordCount = dstream.map(_.value()).flatMap(_.split(" "))
    .filter(!_.isEmpty())
    .map(word => ( word.toLowerCase() , 1))
    .reduceByKey(_+_)
    .updateStateByKey(updateFunc)
  
   
    streamingWordCount.print()
    
    
  // ############################### LOAD DATA TO APACHE HBASE ################################################### //
  
    
    streamingWordCount.foreachRDD(rdd => if(!rdd.isEmpty()) rdd.foreach(send2Hbase(_)))
   
     ssc.checkpoint("hdfs://localhost:9000/SparkCheckpointDir")
    
     ssc.start()   // Start the computation
     ssc.awaitTermination()
    
  }
    
    def send2Hbase(row :(String, Int)) : Unit = 
    {
      
       val table_name = "word_counts"
       val configuration = HBaseConfiguration.create();
       configuration.set("hbase.zookeeper.quorum", "localhost:2182");
       val connection = ConnectionFactory.createConnection(configuration);
        //Specify the target table
       val table = connection.getTable(TableName.valueOf(table_name));
              
     
       val put = new Put(row._1.toString.getBytes)
       
       // Update with new data 
       val newData = row._2.toInt
       print(newData)
            
       put.addColumn("counts".getBytes(), "count".getBytes(), newData.toString().getBytes());
       table.put(put);
      
      
    }
    
  
}
