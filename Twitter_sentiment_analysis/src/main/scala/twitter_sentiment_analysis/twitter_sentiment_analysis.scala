package twitter_sentiment_analysis

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import twitter4j.{Status, TwitterFactory}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import Utils._
import org.apache.spark.streaming.dstream.DStream


object twitter_sentiment_analysis {
  def main(args: Array[String]): Unit = {


    val sparkConfiguration = new SparkConf().
      setAppName("Twitter_sentiment_analysis").
      setMaster(sys.env.get("spark.master").getOrElse("local[*]"))

    // Set up a Spark streaming context named "PrintTweets" that runs locally using
    // all CPU cores and one-second batches of data

    val sparkContext = new SparkContext(sparkConfiguration)
    val ssc = new StreamingContext(sparkContext, Seconds(5))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val filters: Seq[String] = Seq("COVID")
//    val filters_hashtag = args.takeRight(args.length - 4)

    val atwitter: Some[OAuthAuthorization] = OAuthUtils.bootstrapTwitterOAuth()

    val tweets = TwitterUtils.createStream(ssc, atwitter, filters, StorageLevel.MEMORY_AND_DISK_SER_2)

    val uselessWords_en  = sparkContext.broadcast(load("/stop-words-en.dat"))
    val positiveWords_en = sparkContext.broadcast(load("/pos-words-en.dat"))
    val negativeWords_en = sparkContext.broadcast(load("/neg-words-en.dat"))

    val uselessWords_it  = sparkContext.broadcast(load("/stop-words-it.dat"))
    val positiveWords_it = sparkContext.broadcast(load("/pos-words-it.dat"))
    val negativeWords_it = sparkContext.broadcast(load("/neg-words-it.dat"))

    val uselessWords_br  = sparkContext.broadcast(load("/stop-words-br.dat"))
    val positiveWords_br = sparkContext.broadcast(load("/pos-words-br.dat"))
    val negativeWords_br = sparkContext.broadcast(load("/neg-words-br.dat"))


    // Now extract the text of each status update into RDD's using map()
    val englishTweets = tweets.filter(_.getLang() == "en")
    val italianTweets = tweets.filter(_.getLang() == "it")
    val brazilianTweets = tweets.filter(_.getLang() == "pt")

    val textAndSentences: DStream[(TweetText, Sentence)] =
      englishTweets.
        map(_.getText).
        map(tweetText => (tweetText, wordsOf(tweetText)))

    // Apply several transformations that allow us to keep just meaningful sentences
    val textAndMeaningfulSentences: DStream[(TweetText, Sentence)] =
      textAndSentences.
        mapValues(toLowercase).
        mapValues(keepActualWords).
        mapValues(words => keepMeaningfulWords(words, uselessWords_en.value)).
        filter { case (_, sentence) => sentence.length > 0 }

    // Compute the score of each sentence and keep only the non-neutral ones
    val textAndNonNeutralScore: DStream[(TweetText, Int)] =
      textAndMeaningfulSentences.
        mapValues(sentence => computeScore(sentence, positiveWords_en.value, negativeWords_en.value)).
        filter { case (_, score) => score != 0 }

    val textAndNonNeutralScoreHastag: DStream[(Int,String)] =
      textAndNonNeutralScore.
        map { case (tweetText, score)  => (score,retrieveHashtag(hashtagOf(tweetText)))}
        .filter( { case (_, hashtag) => hashtag != " " })


    // Let's check the Italian posts
    val textAndSentences_it: DStream[(TweetText, Sentence)] =
      italianTweets.
        map(_.getText).
        map(tweetText => (tweetText, wordsOf(tweetText)))

    val textAndMeaningfulSentences_it: DStream[(TweetText, Sentence)] =
      textAndSentences_it.
        mapValues(toLowercase).
        mapValues(keepActualWords).
        mapValues(words => keepMeaningfulWords(words, uselessWords_it.value)).
        filter { case (_, sentence) => sentence.length > 0 }

    val textAndNonNeutralScore_it: DStream[(TweetText, Int)] =
      textAndMeaningfulSentences_it.
        mapValues(sentence => computeScore(sentence, positiveWords_it.value, negativeWords_it.value)).
        filter { case (_, score) => score != 0 }

    val textAndNonNeutralScoreHastag_it: DStream[(Int,String)] =
      textAndNonNeutralScore_it.
        map { case (tweetText, score)  => (score,retrieveHashtag(hashtagOf(tweetText)))}.
        filter( { case (_, hashtag) => hashtag != " " })

    // Let's check the Brazilian posts
    val textAndSentences_br: DStream[(TweetText, Sentence)] =
      brazilianTweets.
        map(_.getText).
        map(tweetText => (tweetText, wordsOf(tweetText)))

    val textAndMeaningfulSentences_br: DStream[(TweetText, Sentence)] =
      textAndSentences_br.
        mapValues(toLowercase).
        mapValues(keepActualWords).
        mapValues(words => keepMeaningfulWords(words, uselessWords_br.value)).
        filter { case (_, sentence) => sentence.length > 0 }

    val textAndNonNeutralScore_br: DStream[(TweetText, Int)] =
      textAndMeaningfulSentences_br.
        mapValues(sentence => computeScore(sentence, positiveWords_br.value, negativeWords_br.value)).
        filter { case (_, score) => score != 0 }

    val textAndNonNeutralScoreHastag_br: DStream[(Int,String)] =
      textAndNonNeutralScore_br.
        map { case (tweetText, score)  => (score,retrieveHashtag(hashtagOf(tweetText)))}.
        filter( { case (_, hashtag) => hashtag != " " })

    val topScore60 = textAndNonNeutralScoreHastag.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(160))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    topScore60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nEnglish in last 160 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, (score,hashtag) ) => println("Score: %s ( %s tweets) - Hashtag: %s".format(score, count,hashtag))}
    })

//    val topScore60_it = textAndNonNeutralScoreHastag_it.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(160))
//      .map { case (topic, count) => (count, topic) }
//      .transform(_.sortByKey(false))
//
//    topScore60_it.foreachRDD(rdd => {
//      val topList = rdd.take(10)
//      println("\nItalian sentiment in last 60 seconds (%s total):".format(rdd.count()))
//      topList.foreach { case (count, (_,tag) ) => println("Tweet: %s ( %s tweets)".format(retrieveHashtag(tag), count)) }
//    topList.foreach { case (count, (score,hashtag) ) => println("Score: %s ( %s tweets) - Hashtag: %s".format(score, count,hashtag))}
//    })

      val topScore60_br = textAndNonNeutralScoreHastag_br.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(160))
        .map { case (topic, count) => (count, topic) }
        .transform(_.sortByKey(false))

      topScore60_br.foreachRDD(rdd => {
        val topList = rdd.take(10)
        println("\nBrazilian sentiment in last 160 seconds (%s total):".format(rdd.count()))
        topList.foreach { case (count, (score,hashtag) ) => println("Score: %s ( %s tweets) - Hashtag: %s".format(score, count,hashtag))}
      })


    // tweets.saveAsTextFiles("tweets", "json")

    // Kick it all off
    ssc.start()
    ssc.awaitTermination()
  }

}
