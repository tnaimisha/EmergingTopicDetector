import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils
import com.cybozu.labs.langdetect.DetectorFactory
import org.apache.spark.streaming.dstream.DStream
import twitter4j.Status

import scala.util.Try

object MapWithState extends Serializable {

  def detectLanguage(text: String): String = {

    Try {
      val detector = DetectorFactory.create()
      detector.append(text)
      detector.detect()
    }.getOrElse("unknown")

  }

  /*def detectPopularHashTag(englishTweets: ) : String ={

  }*/
  def main(args: Array[String]) {

    val checkpointDir: String = "/Users/chitti/spark1"
    var TopHash = ""

    if (args.length < 4) {
      System.err.println("Usage: Demo <consumer key> <consumer secret> " + "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val conf = new SparkConf().setAppName("Emerging Topic Detector").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint(checkpointDir)

    DetectorFactory.loadProfile("src/main/resources/profiles")

    @transient val tweets = TwitterUtils.createStream(ssc, None, filters)
    @transient val englishTweets = tweets.filter(Status => detectLanguage(Status.getText).equals("en"))

    /*val data = englishTweets.filter { tweet =>
      val tags = tweet.getText.split(" ").filter(_.startsWith("#"))
      tags.exists(x => true)
    }*/

    @transient val mappingFunc = (key: String, value: Option[Int], state: State[Int]) => {
      val Difference = value.getOrElse(0) - state.getOption().getOrElse(0)
      state.update(value.getOrElse(0))
      (key, Difference)
    }

    /*val myrdd2 = englishTweets.window(Seconds(30),Seconds(30)).map { Status =>
      val Topic = Status.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase())
      val Content = Status.getText
      //val sentiment = SentimentAnalysisUtils.detectSentiment(Status.getText)
      (Topic.mkString(" "), Content)
    }*/

    val myrdd = englishTweets.window(Seconds(30), Seconds(30))
      .flatMap(status => status.getText.split(" ").filter(_.startsWith("#"))).map(_.toLowerCase())
      .map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(30))
      .mapWithState(StateSpec.function(mappingFunc))
      .transform(_.sortBy(_._2, false))
      .checkpoint(Seconds(60))
      .map { case (a, b) => a }
      .transform(rdd => {
        rdd.filter(rdd.take(1).toList.contains)
      })

    //println(myrdd)

    /*def popularTag(tweetStream: DStream[Status]):  DStream[String] = {
      val eTweets = tweetStream.window(Seconds(30), Seconds(30))
        .flatMap(status => status.getText().split(" ").filter(_.startsWith("#")).map(_.toLowerCase()))
        .map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(30))
        .mapWithState(StateSpec.function(mappingFunc))
        .transform(_.sortBy(_._2, false))
        .checkpoint(Seconds(60))
        .map { case (a, b) => a }
        .transform(rdd => {
          rdd.filter(rdd.take(1).toList.contains)})
      eTweets
    }

    val b = popularTag(englishTweets)

    def filtering(popularTag: DStream[String],twitterStream: DStream[Status]): Unit = {
      val popularTopic = twitterStream.filter{status =>
        val tags = status.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase())
        tags.contains(popularTag)
      }

      popularTopic
    }

    @transient val output = filtering(popularTag(englishTweets),englishTweets)

    println(output)

    //val popularTopic = englishTweets.filter(process)*/

    val popularTopic = englishTweets.filter{status =>
      val tags = status.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase())
      tags.equals(myrdd)
    }

    popularTopic.saveAsTextFiles("/Users/chitti/spark1")
 //   println(TopHash)*/

    /*val output = englishTweets.map { Status =>
      val Topic = Status.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase())
      val Content = Status.getText
      //val sentiment = SentimentAnalysisUtils.detectSentiment(Status.getText)
      (Topic.mkString(" "), Content)
    }*/

    /*output.foreachRDD { rdd =>
      rdd.foreach(x => println("Topic: " + x._1 + "\t\tContent: " + x._2 + "\t\tSentiment:" + x._3))
    }*/

    /*englishTweets.foreachRDD{ rdd => {
      val Topic = rdd.filter(_.getHashtagEntities.map(_.getText).equals(myrdd2))
      println("Topic" + Topic)
    }*/


    ssc.start()
    ssc.awaitTermination()


  }

}
