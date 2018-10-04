package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._


object KafkaSpark {
  def main(args: Array[String]) {

    println("Hej, I am running!")
    //We use direct, so no need for a dedicated thread for the receiver, but we still keep two.
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    println("Keyspace Created")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float)")
    println("Table Created")
//
//    // make a connection to Kafka and read (key, value) pairs from it
    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")
    val topics = Set("avg")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topics)
    //messages is now a discretized RDD
    def getTheTuple(x:String) : (String, Double) = {
      val splitted = x.split(",")
      (splitted(0), splitted(1).toDouble)
    }
    val pairs = messages.map(x => x._2).map(getTheTuple)
//    pairs.print // Good for debugging

    // measure the average value for each key in a stateful manner
    // I don't think the state should be a double, as it would be fairly difficult to compute the average
    // by not knowing the number of elements computed so-far
    // The state should contain the count and the sum of the values
    // then computes the avg, saves the state and then emits the avg
    // https://databricks.com/blog/2016/02/01/faster-stateful-stream-processing-in-apache-spark-streaming.html
    def mappingFunc(key: String, value: Option[Double], state: State[(Double,Double)]): (String, Double) = {

      // this is not very scala-ish, it should be handled with pattern matching and options, I am still not comfortable
      // with them. Should dedicate more time to scala.
      val sanitizedValue = value.getOrElse(0D) //CAREFUL TO PUT THE 0D Otherwise it could be either Double or Int! and the sum won't work!
      var valueCount = 0D
      var valueSum = 0D
      if (state.exists){ //Obviously, it may not exist, the first time we get a key, or in case of timeouts (not our case)
        valueCount = state.get()._1
        valueSum = state.get()._2
      }

      val newValueSum = valueSum + sanitizedValue
      val newValueCount = valueCount + 1
      state.update((newValueCount, newValueSum))
      val avg = newValueSum/newValueCount
//      if (key == "t") // debugging on one key;
//      {
//        println(s"Key: $key; Value: $sanitizedValue; ValueCount: $valueCount; ValueSum: $valueSum; NVC: $newValueCount; NVS: $newValueSum; avg: $avg")
//      }
      (key, avg)
    }
    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

//
    // store the result in Cassandra
    // just need the keyspace and table name
    stateDstream.saveToCassandra("avg_space", "avg")
//
//    stateDstream.print
    ssc.checkpoint("/tmp") // in order to use stateful data you need to set the checkpoint directory
    ssc.start()
    ssc.awaitTermination()
//    println("Done")
  }
}
