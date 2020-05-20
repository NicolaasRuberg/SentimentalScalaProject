package twitter_sentiment_analysis

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter._
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.auth.OAuthAuthorization
import twitter_sentiment_analysis.Utils._

object twitter_sentiment_analysis_Nic {
  def main(args: Array[String]): Unit = {

    val sparkConfiguration = new SparkConf().
      setAppName("Twitter_sentiment_analysis").
      setMaster(sys.env.get("spark.master").getOrElse("local[*]"))

    val sparkContext = new SparkContext(sparkConfiguration)
    val ssc = new StreamingContext(sparkContext, Seconds(5))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val filters: Seq[String] = Seq("#COVID")
    val filters_hashtag = args.takeRight(args.length - 4)

    val atwitter: Some[OAuthAuthorization] = OAuthUtils.bootstrapTwitterOAuth()

    val tweets = TwitterUtils.createStream(ssc, atwitter, filters, StorageLevel.MEMORY_AND_DISK_SER_2)

    val uselessWords  = sparkContext.broadcast(load("/stop-words.dat"))
    val positiveWords = sparkContext.broadcast(load("/pos-words.dat"))
    val negativeWords = sparkContext.broadcast(load("/neg-words.dat"))

    // Now extract the text of each status update into RDD's using map()

//    val englishTweets = tweets.filter(_.getLang() == "en")
//
//    val status = tweets.map(status => status.getText)
//    englishTweets.foreachRDD(rdd => {
//      rdd.map(status => {
//        //println(s)
//        var tweet_current = "";
//        if(status.getRetweetedStatus != null)
//          tweet_current = status.getRetweetedStatus().getText
//        else
//          tweet_current = status.getText
//
//        println("")
//        tweet_current
//
//      }).foreach(println)
//    })

//    val stream = TwitterUtils.createStream(ssc, atwitter, filters_hashtag, StorageLevel.MEMORY_AND_DISK_SER_2)
//  val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    val englishTweets = tweets.filter(_.getLang() == "en")

 //*

    val textAndSentences: DStream[(TweetText, Sentence)] =
      englishTweets.
        map(_.getText).
        map(tweetText => (tweetText, wordsOf(tweetText)))

    // Apply several transformations that allow us to keep just meaningful sentences
    val textAndMeaningfulSentences: DStream[(TweetText, Sentence)] =
      textAndSentences.
        mapValues(toLowercase).
        mapValues(keepActualWords).
        mapValues(words => keepMeaningfulWords(words, uselessWords.value)).
        filter { case (_, sentence) => sentence.length > 0 }

    // Compute the score of each sentence and keep only the non-neutral ones
    val textAndNonNeutralScore: DStream[(TweetText, Int)] =
      textAndMeaningfulSentences.
        mapValues(sentence => computeScore(sentence, positiveWords.value, negativeWords.value)).
        filter { case (_, score) => score != 0 }

    // Transform the (tweet, score) pair into a readable string and print it
    textAndNonNeutralScore.foreachRDD(rdd => {
        rdd.collect().foreach { case (_, score) =>
          println("\nTweet sentiment score: %s.".format(score))
        }
    })
//*
    val hashTags = englishTweets.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    val topScore60 = textAndNonNeutralScore.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))
      .filter { case (_, sentence) => sentence.length > 0 }

    topScore60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nMy in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, (tag,score)) => println("%s (%s tweets)".format(score, count)) }
    })

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


    englishTweets.cache()
    englishTweets.foreachRDD {rdd =>
      if (rdd != null && !rdd.isEmpty() && !rdd.partitions.isEmpty){
        saveRawTweetsInJSONFormat(rdd, "./src/main/Out/")
      }
    }

//    tweets.saveAsTextFiles("tweets", "json")

    // Kick it all off
    ssc.start()
//    ssc.awaitTermination()
    val minutes=1
    ssc.awaitTerminationOrTimeout(minutes*60*1000)
  }

}
