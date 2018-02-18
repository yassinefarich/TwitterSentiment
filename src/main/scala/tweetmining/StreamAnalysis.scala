package tweetmining

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD

object StreamAnalysis {
  val conf = new SparkConf().setMaster("local[6]").setAppName("Spark batch processing")
  val sc = new SparkContext(conf)
  val streamingContext = new StreamingContext(sc, Seconds(10))
  sc.setLogLevel("WARN")
  def main(args: Array[String]): Unit = {
    println("Twitter data stream processing")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "twitter-consumer",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array("tweets")
    val tweetStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
      

      //tweetStream.filter(tweet => tweet.value().contains("#DonaldTrump")).foreachRDD(rDD => println(rDD.count()));
    
      tweetStream.map(tweet => tweet.value())
                 .map(tweet => (tweet, TweetUtilities.getSentiment(tweet)))
                 .flatMap(tweetSentiment => TweetUtilities.getHashTags(tweetSentiment._1).map(tag => (tag, tweetSentiment._2)))
                 .reduceByKey((v1,v2)=>v1+v2)
                 .map(tweetSentiment => tweetSentiment.swap)
                 .foreachRDD(rDD => {
                     println("\n### Top 5 ###")
                     rDD.top(5).foreach(println)
                   }) ;
      
      tweetStream.map(tweet => tweet.value())
                 .map(tweet => (tweet, TweetUtilities.getSentiment(tweet)))
                 .flatMap(tweetSentiment => TweetUtilities.getHashTags(tweetSentiment._1).map(tag => (tag, tweetSentiment._2)))
                 .reduceByKey((v1,v2)=>v1+v2)
                 .map(tweetSentiment => tweetSentiment.swap)
                 .foreachRDD(rDD => {
                     println("### TakeOrdered 5 ###")
                     rDD.takeOrdered(5).foreach(println)
                   });
      
    
    
    //add your code here

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}