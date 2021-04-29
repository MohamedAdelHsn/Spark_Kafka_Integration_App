# Spark_Kafka_Integration_App

### How to run the app 
To integrate spark streaming with kafka <br />
* First -> 
 you need to install kafka in your machine go to [kafka_Downloads](https://www.apache.org/dyn/closer.cgi?path=/kafka/2.8.0/kafka_2.13-2.8.0.tgz) and download kafka_2.13-   2.8.0.tgz <br />
 
 Try to extract files using tar
 
 ```
  tar -xvf /path../kafka_2.13-2.8.0.tgz
 ```
 
 Try to put kafka in ~/.bashrc using 
  
  ```
  sudo nano ~/.bashrc
  export KAFKA_HOME=/home/hdpadmin/kafka_2.13-2.8.0
  export PATH=$PATH:$KAFKA_HOME/bin
  source ~/.bashrc
  
  ```
  
 To start subscribe or publish messages into kafka topics 
  - First ->  start zookeper server by : ``` cd $KAFKA_HOME; bin/zookeeper-server-start.sh config/zookeeper.properties ```
  - Second -> start Kafka Server by   : ```  bin/kafka-server-start.sh config/server.properties ```
  - Third ->  create kafka topic  by  : ```  bin/kafka-topics.sh --create --topic myFirstTopic --bootstrap-server localhost:9092 ```
  - Fourth -> create kafka producer by : ``` bin/kafka-console-producer.sh --topic myFirstTopic --bootstrap-server localhost:9092 ``` 
  

Then Conusme these messages by SparkStreaming if use maven try this 
  ```
  <dependency>  
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-core_2.12</artifactId>
    <version>3.1.1</version>
  </dependency>
  
  <--dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-streaming_2.12</artifactId>
    <version>3.1.1</version>
    <scope>provided</scope>
 </dependency>

 <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10 -->
 <dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-streaming-kafka-0-10_2.12</artifactId>
    <version>3.1.1</version>
 </dependency>

  ```
 
 Then write your SparkStreaming_kafka App [go to here](https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10_2.12/3.1.1)


 

