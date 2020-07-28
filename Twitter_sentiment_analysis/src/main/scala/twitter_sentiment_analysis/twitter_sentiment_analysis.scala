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
import java.util.{Calendar, Date}
import java.text.SimpleDateFormat



object twitter_sentiment_analysis {

  def main(args: Array[String]): Unit = {

    val timeWindow: Int = 1

    val sparkConfiguration = new SparkConf().
      setAppName("Twitter_sentiment_analysis")

    // Check if we're running on the AWS or locally, set the dictionaries path accordingly
    var dictPath = ""
    if (! sys.env.get("HOME").toString.contains("bia" ))
      dictPath = "s3://unibo-scala-twitter/"
    else {
       dictPath = "./"
      sparkConfiguration.setMaster(sys.env.get("spark.master").getOrElse("local[*]"))
    }
    println("dictPATH: %s".format(dictPath))

    // Set up a Spark streaming context named "PrintTweets" that runs locally using
    // all CPU cores and one-second batches of data

    val sparkContext = new SparkContext(sparkConfiguration)
    val ssc = new StreamingContext(sparkContext, Seconds(5))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val filters: Seq[String] = Seq("COVID")

    val atwitter: Some[OAuthAuthorization] = OAuthUtils.bootstrapTwitterOAuth()

    val tweets = TwitterUtils.createStream(ssc, atwitter, filters, StorageLevel.MEMORY_AND_DISK_SER_2)

    val uselessWords_en  = sparkContext.textFile(dictPath + "/Resources/stop-words-en.dat").collect().toSet
    val positiveWords_en = sparkContext.textFile(dictPath + "/Resources/pos-words-en.dat").collect().toSet
    val negativeWords_en = sparkContext.textFile(dictPath + "/Resources/neg-words-en.dat").collect().toSet
    val uselessWords_it  = sparkContext.textFile(dictPath + "/Resources/stop-words-it.dat").collect().toSet
    val positiveWords_it = sparkContext.textFile(dictPath + "/Resources/pos-words-it.dat").collect().toSet
    val negativeWords_it = sparkContext.textFile(dictPath + "/Resources/neg-words-it.dat").collect().toSet
    val uselessWords_br  = sparkContext.textFile(dictPath + "/Resources/stop-words-br.dat").collect().toSet
    val positiveWords_br = sparkContext.textFile(dictPath + "/Resources/pos-words-br.dat").collect().toSet
    val negativeWords_br = sparkContext.textFile(dictPath + "/Resources/neg-words-br.dat").collect().toSet

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
        mapValues(words => keepMeaningfulWords(words, uselessWords_en)).
        filter { case (_, sentence) => sentence.length > 0 }

    // Compute the score of each sentence and keep only the non-neutral ones
    val textAndNonNeutralScore: DStream[(TweetText, Int)] =
      textAndMeaningfulSentences.
        mapValues(sentence => computeScore(sentence, positiveWords_en, negativeWords_en)).
        filter { case (_, score) => score != 0 }

    val textAndNonNeutralScoreHastag: DStream[(Int,String)] =
      textAndNonNeutralScore.
        map { case (tweetText, score)  => (score,retrieveHashtag(hashtagOf(tweetText)))}
        .filter( { case (_, hashtag) => hashtag != " " })


    // Let's check the Italian posts
    val textAndNonNeutralScoreHastag_it: DStream[(Int, String)] =
      italianTweets.
        map(_.getText).
        map(tweetText => (tweetText, wordsOf(tweetText))).
        mapValues(getMeaninfulSenteces(_,uselessWords_it)).
        filter { case (_, sentence) => sentence.length > 0 }.
        mapValues(sentence => computeScore(sentence, positiveWords_it, negativeWords_it)).
        filter { case (_, score) => score != 0 }.
        map { case (tweetText, score)  => (score,retrieveHashtag(hashtagOf(tweetText)))}.
        filter( { case (_, hashtag) => hashtag != " " })

    // Let's check the Brazilian posts
    val textAndNonNeutralScoreHastag_br: DStream[(Int, String)] =
      brazilianTweets.
        map(_.getText).
        map(tweetText => (tweetText, wordsOf(tweetText))).
        mapValues(getMeaninfulSenteces(_,uselessWords_br)).
        filter { case (_, sentence) => sentence.length > 0 }.
        mapValues(sentence => computeScore(sentence, positiveWords_br, negativeWords_br)).
        filter { case (_, score) => score != 0 }.
        map { case (tweetText, score)  => (score,retrieveHashtag(hashtagOf(tweetText)))}.
        filter( { case (_, hashtag) => hashtag != " " })

    val topScore60 = textAndNonNeutralScoreHastag.map((_, 1)).reduceByKeyAndWindow(_ + _, Minutes(timeWindow))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))
    //  Prepare english file for out put
    val myFile = dictPath+"out/" + "output-en"
    val sdf = new SimpleDateFormat("yy-MM-dd hh:mm:ss")

    topScore60.foreachRDD(rdd => {
      if (rdd.take(1).length != 0)
        println("####en; %s; %s;%s".format(sdf.format(new Date), rdd.count(),rdd.reduce(
          (a,b) => (0,((a._2._1 + b._2._1),"")))._2._1))
    })

//    topScore60.map({ case (count,(score,hashtag: Hashtag))  =>
//      (sdf.format(new Date),(count + count),(score + score))}).
//      print()
////      saveAsTextFiles(myFile)

//    topScore60.foreachRDD(rdd => {
//      val topList = rdd.take(100)
//      println("\nEnglish in last %s min. (%s total):".format(timeWindow, rdd.count()))
//      topList.foreach { case (count, (score,hashtag) ) => println("Score: %s ( %s tweets) - Hashtag: %s".format(score, count,hashtag))}
//    })

    val topScore60_it = textAndNonNeutralScoreHastag_it.map((_, 1)).reduceByKeyAndWindow(_ + _, Minutes(timeWindow))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    topScore60_it.foreachRDD(rdd => {
      if (rdd.take(1).length != 0)
        println("####it; %s; %s;%s".format(sdf.format(new Date), rdd.count(),rdd.reduce(
          (a,b) => (0,((a._2._1 + b._2._1),"")))._2._1))
    })

//    val itFile = dictPath+"out/" + "output-it"
//    topScore60_it.map({ case (count,(score,hashtag: Hashtag))  =>
//      (sdf.format(new Date),(count + count),(score + score))}).
//      print()
////      saveAsTextFiles(itFile)

//    topScore60_it.foreachRDD(rdd => {
//      val topList = rdd.take(10)
//      println("\nItalian sentiment in last %s min. (%s total):".format(timeWindow,rdd.count()))
//      topList.foreach { case (count, (score,hashtag) ) => println("Score: %s ( %s tweets) - Hashtag: %s".format(score, count,hashtag))}
//    })

    val topScore60_br = textAndNonNeutralScoreHastag_br.map((_, 1)).reduceByKeyAndWindow(_ + _, Minutes(timeWindow))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    topScore60_br.foreachRDD(rdd => {
      if (rdd.take(1).length != 0)
        println("####br; %s; %s;%s".format(sdf.format(new Date), rdd.count(),rdd.reduce(
        (a,b) => (0,((a._2._1 + b._2._1),"")))._2._1))
    })

//    topScore60_br.foreachRDD(rdd => {
//      val topList = rdd.take(10)
//      println("\nBrazilian sentiment in last %s min. (%s total):".format(timeWindow,rdd.count()))
//      topList.foreach { case (count, (score,hashtag) ) => println("Score: %s ( %s tweets) - Hashtag: %s".format(score, count,hashtag))}
//    })

//    val brFile = dictPath+"out/" + "output-br"
//    topScore60_br.map({ case (count,(score,hashtag: Hashtag))  =>
//      (sdf.format(new Date),(count + count),(score + score))}).
//      print()
////      saveAsTextFiles(brFile)

    // tweets.saveAsTextFiles("tweets", "json")
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
