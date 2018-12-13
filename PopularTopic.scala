import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

object PopularTopic {

  def main(args: Array[String]){

    if (args.length<4){
      System.err.println("Usage: Demo <consumer key> <consumer secret> " + "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length-4)

    System.setProperty("twitter4j.oauth.consumerKey",consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret",consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken",accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret",accessTokenSecret)

    val conf = new SparkConf().setAppName("Emerging Topic Detector").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(10))

    val tweets = TwitterUtils.createStream(ssc, None, filters)

    val englishTweets = tweets.filter(_.getLang == "en")

    val hashTags = englishTweets.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topCounts60 = hashTags.map((_,1)).reduceByKeyAndWindow(_ + _, Seconds(60))
        .map{case(topic,count) => (count,topic)}
        .transform(_.sortByKey(false))

    topCounts60.foreachRDD(rdd => {rdd.foreach{case(count,topic) => println("%s (%s)" .format(count,topic))}})

    /*topCounts60.foreachRDD(rdd => {
      val top10 = rdd.take(10)
      println("\nNumber of popular topics in last 60 secs: %s " .format(rdd.count()))
      top10.foreach{case(count,topic) => println("%s (%s)" .format(count,topic))}
    }
    )*/

    //englishTweets.print(50)
    //hashTags.print()

    ssc.start()
    ssc.awaitTermination()


  }

}

