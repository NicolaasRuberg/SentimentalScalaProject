package twitter_sentiment_analysis

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import twitter4j.TwitterFactory
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object twitter_sentiment_analysis {
  def main(args: Array[String]): Unit = {
    var conf2 = new SparkConf()
    conf2.setMaster("local")
    conf2.setAppName("Second Application")

    //    val sc2 = new SparkContext(conf2)
    //
    //    // Create a RDD
    //
    //    val rdd2 = sc2.makeRDD(Array(1, 2, 3, 4, 5, 6, 7))
    //    rdd2.collect().foreach(println)
    // Configure Twitter credentials

    // Set up a Spark streaming context named "PrintTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(5))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val filters: Seq[String] = Seq("TRUMP")
    val filters_hashtag = args.takeRight(args.length - 4)


    val cb = new ConfigurationBuilder()
      .setOAuthConsumerKey("Blow2DwwbT4MAujtmFZuXKCOQ")
      .setOAuthConsumerSecret("hfJIsPNeaBZ3qsfXoAzFEqT5Y1yxqq75gnSQJ4XjECE3m3DxyO")
      .setOAuthAccessToken("1255521908436750341-vtkCY6FxxPvyD6AAwsopkfM6eDXRHK")
      .setOAuthAccessTokenSecret("mHHuR17dG7HWaQBDyQ0yl4vZ8l8nBovmU1oe37xyDFVJs").build()

    val twitter_auth = new TwitterFactory(cb)
    val a = new OAuthAuthorization(cb)
    val atwitter: Option[twitter4j.auth.Authorization] = Some(twitter_auth.getInstance(a).getAuthorization())

    val tweets = TwitterUtils.createStream(ssc, atwitter, filters, StorageLevel.MEMORY_AND_DISK_SER_2)
    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(status => status.getText())

    // Print out the first ten
    statuses.print()


    val stream = TwitterUtils.createStream(ssc, atwitter, filters_hashtag, StorageLevel.MEMORY_AND_DISK_SER_2)
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })
    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })


    //    tweets.saveAsTextFiles("tweets", "json")

    // Kick it all off
    ssc.start()
    ssc.awaitTermination()
  }

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

}
