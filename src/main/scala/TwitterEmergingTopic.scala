import com.cybozu.labs.langdetect.DetectorFactory
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming._


import scala.util.Try

object TwitterEmergingTopic {

  //This function is used for detecting language of the content passed to it.
  def detectLanguage(text: String): String = {

    Try {
      val detector = DetectorFactory.create()
      detector.append(text)
      detector.detect()
    }.getOrElse("unknown")

  }

  def main(args: Array[String]) {

    // Parse input argument for accessing twitter streaming api
    if (args.length < 4) {
      System.err.println("Usage: Demo <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    // Set the system properties for the Twitter4j Library. Used for generating OAuth Credentials
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Set up streaming-context
    val conf = new SparkConf().setAppName("Demo").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(10))

    // Check-pointing for stateful streaming
    val checkpointDir: String = "/Users/chitti/spark1"
    ssc.checkpoint(checkpointDir)

    DetectorFactory.loadProfile("src/main/resources/profiles")

    // Creating twitter input stream and filtering the tweets that contain hashtags
    val tweets = TwitterUtils.createStream(ssc, None, filters)
    val englishTweets = tweets.filter(Status => detectLanguage(Status.getText).equals("en"))
      .filter { tweet =>
        val tags = tweet.getText.replace("\n"," ").split(" ").filter(_.startsWith("#"))
        tags.exists(x => true)
      }

    // fetching the (hashtag,tweet) for joining and filtering, based on popular hashtags
    val hashTagTweetTuple = englishTweets.window(Minutes(1), Minutes(1)).map { tweets =>
      val Topics = tweets.getText.replace("\n"," ").split(" ").filter(_.startsWith("#")).map(_.toLowerCase()).toList
      val Content = tweets.getText
      (Content, Topics)
    }
    val joinrdd2 = hashTagTweetTuple.flatMap(x => x._2.map(y => (x._1, y)))
      .map { case (a, b) => (b, a) }

    // Mapping function used by MapWithState. The hashtag count difference between the current state and the previous state is calculated
    val mappingFunc = (key: String, value: Option[Int], state: State[Int]) => {
      val difference = value.getOrElse(0) - state.getOption().getOrElse(0)
      state.update(value.getOrElse(0))
      (key, difference)
    }

    // Fetching the top HashTag with mapWithState stateful streaming
    val topHashTag = englishTweets.window(Minutes(1), Minutes(1))
      .flatMap(status => status.getText.replace("\n"," ").split(" ").filter(_.startsWith("#"))).map(_.toLowerCase())
      .map((_, 1)).reduceByKeyAndWindow(_ + _, Minutes(1))
      .mapWithState(StateSpec.function(mappingFunc))
      .transform(_.sortBy(_._2, false))
      .checkpoint(Minutes(2))
      .transform(rdd => {
        rdd.filter(rdd.take(1).toList.contains)
      })

    // Filtering the tweets based on popular Hashtag
    val popularTweets = topHashTag.join(joinrdd2)

    val output = popularTweets.map { x =>
      val Topic = x._1
      val Content = x._2._2
      val Sentiment = SentimentAnalysisUtils.detectSentiment(Content)
      (Topic, Content, Sentiment.toString)
    }

    // printing the output for each sliding window
    output.foreachRDD { rdd =>
      rdd.foreach(x => println("Topic: " + x._1 + "\nContent: " + x._2 + "\nSentiment:  " + x._3 + "\n\n"))
      rdd.map(t => {
        Map(
          "Topic" -> t._1,
          "Content" -> t._2,
          "Sentiment" -> t._3
        )
      }).coalesce(1).saveAsTextFile("/Users/chitti/spark/twitteremerging.txt")
    }

    ssc.start()
    ssc.awaitTermination()


  }

}

