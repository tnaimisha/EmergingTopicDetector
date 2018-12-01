import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils
import com.cybozu.labs.langdetect.DetectorFactory
import scala.util.Try

object MapWithState {

  def main(args: Array[String]) {

    var maxDifference = 0
    var popularHashTag: String = ""

    val checkpointDir: String = "/Users/chitti/spark1"

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

    def detectLanguage(text: String) : String = {

      Try {
        val detector = DetectorFactory.create()
        detector.append(text)
        detector.detect()
      }.getOrElse("unknown")

    }

    val tweets = TwitterUtils.createStream(ssc, None, filters)
    val englishTweets = tweets.filter(Status => detectLanguage(Status.getText).equals("en"))

    /*val data = englishTweets.filter { tweet =>
      val tags = tweet.getText.split(" ").filter(_.startsWith("#"))
      tags.exists(x => true)
    }*/

    val mappingFunc = (key: String, value: Option[Int], state: State[Int]) => {

      println("Popular HashTag: " + popularHashTag)
      if (maxDifference < (value.getOrElse(0)) - state.getOption().getOrElse(0)) {
        maxDifference = value.getOrElse(0) - state.getOption().getOrElse(0)
        popularHashTag = key
        println("key: " + key + "value: " + value.getOrElse(0))
        println("Popular HashTag: " + popularHashTag)
      }

      state.update(value.getOrElse(0))
      (popularHashTag,maxDifference)
    }

   val myrdd = englishTweets.window(Seconds(30),Seconds(30))
       .flatMap(status => status.getText.split(" ").filter(_.startsWith("#"))).map(_.toLowerCase())
        .map((_,1)).reduceByKeyAndWindow(_+_,Seconds(30))

    myrdd.saveAsTextFiles("/Users/chitti/spark1/reduceop")

   val myrdd2 = myrdd
        .mapWithState(StateSpec.function(mappingFunc))
       .checkpoint(Seconds(60))

    myrdd2.saveAsTextFiles("/Users/chitti/spark1/populartags")

    val popularTopic = englishTweets.filter(_.getHashtagEntities.map(_.getText) == popularHashTag)

    val output = popularTopic.map { Status =>
       val Topic = Status.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase())
       val Content = Status.getText
       val sentiment = SentimentAnalysisUtils.detectSentiment(Status.getText)
       (Topic.mkString(" "), Content, sentiment.toString)
     }

     output.foreachRDD { rdd =>
       rdd.foreach(x => println("Topic: " + x._1 + "\t\tContent: " + x._2 + "\t\tSentiment:" + x._3))
     }

    ssc.start()
    ssc.awaitTermination()


  }

}
