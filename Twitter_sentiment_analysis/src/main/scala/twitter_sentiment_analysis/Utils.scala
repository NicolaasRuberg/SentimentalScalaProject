package twitter_sentiment_analysis

import java.io.InputStream

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import twitter4j.Status

import scala.io.{AnsiColor, Source}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.ReceiverInputDStream



/**
 * Lazily instantiated singleton instance of SQLContext.
 */
object SQLContextSingleton {

  @transient
  @volatile private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = SQLContext.getOrCreate(sparkContext)
        }
      }
    }
    instance
  }
}

object PropertiesLoader {
  val consumerKey = "Blow2DwwbT4MAujtmFZuXKCOQ"
  val consumerSecret = "hfJIsPNeaBZ3qsfXoAzFEqT5Y1yxqq75gnSQJ4XjECE3m3DxyO"
  val accessToken = "1255521908436750341-vtkCY6FxxPvyD6AAwsopkfM6eDXRHK"
  val accessTokenSecret = "mHHuR17dG7HWaQBDyQ0yl4vZ8l8nBovmU1oe37xyDFVJs"

}

/**
 * Helper class for loading Twitter App OAuth Credentials into the VM.
 * The required values for various keys are picked from a properties file in the classpath.
 */
object OAuthUtils {
  def bootstrapTwitterOAuth(): Some[OAuthAuthorization] = {
    System.setProperty("twitter4j.oauth.consumerKey", PropertiesLoader.consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", PropertiesLoader.consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", PropertiesLoader.accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", PropertiesLoader.accessTokenSecret)

    val configurationBuilder = new ConfigurationBuilder()
    val oAuth = Some(new OAuthAuthorization(configurationBuilder.build()))

    oAuth
  }
}

object Utils {

  // Some type aliases to give a little bit of context
  type Tweet = Status
  type TweetText = String
  type Hashtag = String
  type Sentence = Seq[String]

  private def format(n: Int): String = f"$n%2d"

  private def wrapScore(s: String): String = s"[ $s ] "

  private def makeReadable(n: Int): String =
    if (n > 0) s"${AnsiColor.GREEN + format(n) + AnsiColor.RESET}"
    else if (n < 0) s"${AnsiColor.RED + format(n) + AnsiColor.RESET}"
    else s"${format(n)}"

  private def makeReadable(s: String): String =
    s.takeWhile(_ != '\n').take(144) + "..."

  def makeReadable(sn: (String, Int)): String =
    sn match {
      case (tweetText, score) => s"${wrapScore(makeReadable(score))}${makeReadable(tweetText)}"
    }

  def load(resourcePath: String): Set[String] = {
//    val source = Source.fromInputStream(getClass.getResourceAsStream(resourcePath))
    val source = Source.fromFile(resourcePath)
    val words = source.getLines.toSet
    source.close()
    words
  }

  def wordsOf(tweet: TweetText): Sentence =
    tweet.split(" ")

  def hashtagOf(tweet: TweetText): Sentence =
    tweet.split(" ").filter(_.startsWith("#"))

  def toLowercase(sentence: Sentence): Sentence =
    sentence.map(_.toLowerCase)

  def keepActualWords(sentence: Sentence): Sentence =
    sentence.filter(_.matches("[a-z]+"))

  def extractWords(sentence: Sentence): Sentence =
    sentence.map(_.toLowerCase).filter(_.matches("[a-z]+"))

  def keepMeaningfulWords(sentence: Sentence, uselessWords: Set[String]): Sentence =
    sentence.filterNot(word => uselessWords.contains(word))

  def keepMeaningfulWords2(sentence: Sentence, uselessWords: RDD[String]): Sentence = {
    sentence.filterNot(word => uselessWords.toString() == word)
  }
  def computeScore2(words: Sentence, positiveWords: RDD[String], negativeWords: RDD[String]): Int =
    words.map(word => computeWordScore(word, positiveWords.collect().toSet, negativeWords.collect().toSet)).sum

  def getMeaninfulSenteces(tweet: Sentence,uselessWords: Set[String]):Sentence = {
    val lowTweet = toLowercase(tweet)
    val actualWords = keepActualWords(lowTweet)
    keepMeaningfulWords(actualWords, uselessWords)
  }

  def computeScore(words: Sentence, positiveWords: Set[String], negativeWords: Set[String]): Int =
    words.map(word => computeWordScore(word, positiveWords, negativeWords)).sum

  def computeWordScore(word: String, positiveWords: Set[String], negativeWords: Set[String]): Int =
    if (positiveWords.contains(word)) 1
    else if (negativeWords.contains(word)) -1
    else 0

  def retrieveHashtag(list_hashtags: Sentence): String = {
    val counts: Map[String, Int] = list_hashtags.foldLeft(Map.empty[String, Int]) { (map, string) =>
      val count: Int = map.getOrElse(string, 0) //get the current count of the string
      map.updated(string, count + 1) //update the map by incrementing string's counter
    }

    val sortedFrequency: Vector[(String, Int)] = counts.toVector.sortWith(_._2 > _._2)
    if (sortedFrequency.length == 0)
      return (" ")
    else
      sortedFrequency(0)._1
  }


  val jacksonObjectMapper: ObjectMapper = new ObjectMapper()
  /**
   * Saves raw tweets received from Twitter Streaming API in
   *
   * @param rdd           -- RDD of Status objects to save.
   * @param tweetsRawPath -- Path of the folder where raw tweets are saved.
   */

  def saveRawTweetsInJSONFormat(rdd: RDD[Status], tweetsRawPath: String): Unit = {
    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    val tweet = rdd.map(status => jacksonObjectMapper.writeValueAsString(status))
    val rawTweetsDF = sqlContext.read.json(tweet)
    rawTweetsDF.coalesce(1).write
      .format("org.apache.spark.sql.json")
      // Compression codec to compress when saving to file.
      .option("codec", classOf[GzipCodec].getCanonicalName)
      .mode(SaveMode.Append)
      .save(tweetsRawPath)
  }

  def replaceNewLines(tweetText: String): String = {
    tweetText.replaceAll("\n", "")
  }


}
