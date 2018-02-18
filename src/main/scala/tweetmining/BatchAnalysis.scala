package tweetmining

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.mapreduce.Reducer

object BatchAnalysis {
  val conf = new SparkConf().setMaster("local[6]").setAppName("Spark batch processing")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  def main(args: Array[String]): Unit = {

    println("Twitter data batch processing")
    val tweets: RDD[String] = sc.textFile("1Mtweets_en.txt")

    //1. How many tweets mention Donald Trump
    println(s"#DonaldTrump : ${tweets.filter(tweet => tweet.contains("#DonaldTrump")).count()}");

    //2. Build a RDD that contains (tweet, sentiment) pairs indicating the sentiment of each tweet. To
    //  check that it is working properly, display the content of 5 elements (
    //  take(5).foreach(println)).

    val tweetSentiment = tweets.map(tweet => (tweet, TweetUtilities.getSentiment(tweet)))
    //tweetSentiment.take(5).foreach(println);

    //3. Using the previous RDD, create a RDD that represents the sentiment associated to each
    //hashtag (or mention). Once again, display a few elements to check things work properly

    val sentimentByTweet = tweetSentiment.flatMap(tweetSentiment => TweetUtilities.getHashTags(tweetSentiment._1)
                                         .map(tag => (tag, tweetSentiment._2)))
                                         .reduceByKey((v1,v2)=>v1+v2)
                                         
    //sentimentByTweet.take(5).foreach(println)
    
    //4. Now we wish to see the k most positive / negative hashtags. takeOrdered(k) gives you the k
    //smallest elements according to the key, while top(k) gives you the k highest. When working
    //on a pair, swap can be used to exchange the position of the key and the value.
    
    val top5Sentiments = sentimentByTweet.map(tweetSentiment => tweetSentiment.swap).top(5)
    top5Sentiments.foreach(println)
    
    println("##### #####")
    
    val last5Sentiments = sentimentByTweet.map(tweetSentiment => tweetSentiment.swap).takeOrdered(5)
    last5Sentiments.foreach(println)
    
   // sentimentByTweet.top(5).foreach(println)

    //tweets.flatMap( tweetSentiment => TweetUtilities.getHashTags(tweetSentiment)).map(hashTag => (hashTag,TweetUtilities.getSentiment(hashTag))).take(5).foreach(println);

  }
}