package spark.streaming.kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
  * 启动kafka
  * kafka-server-start.sh /mnt/d/usr/local/kafka/config/server.properties
  *
  * 创建生产者
  * kafka-console-producer.sh --topic gandi --broker-list localhost:9092
  *
  * submit命令
  * ./bin/spark-submit --class spark.streaming.kafka.KafkaWordCount --master local[2] --jars jars/spark-streaming-kafka-0-10-assembly_2.11-2.4.3.jar,jars/kafka-clients-2.3.1.jar /mnt/d/IdeaProjects/SparkDemo/target/SparkDemo-1.0.jar localhost:9092 t gandi
  *
  *
  */
object KafkaWordCount {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics>")
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    val Array(zkQuorum, group, topics) = args
    val topicsArr = topics.split(",")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> zkQuorum,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsArr, kafkaParams)
    )
    val lines = stream.map(_.value())
    val wordCount = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()

  }

}
