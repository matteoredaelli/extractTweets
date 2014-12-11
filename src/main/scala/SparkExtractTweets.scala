import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._

import java.text.SimpleDateFormat
import java.text.DateFormat
import java.util.Date

object SparkExtractTweets {

  def tweet2Html(tweet:(Any,Any,Any,Any)) {
    val (date, user, id, text) = tweet
    println(s"<a href=https://twitter.com/$user/status/$id>$date</a>: $text<br />");
  }

/*
  t0(0).asInstanceOf[scala.collection.mutable.ArrayBuffer[String]].getClass)

  def extractTags(hashtagEntity: Array[String]) : String = {
     if( hashtagEntities.asInstanceOf[scala.collection.mutable.ArrayBuffer[Any].length > 3) {
        return hashtagEntity(2) 
     } else {
        return ""
     }
  }
*/

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("RDDRelation")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val filename = "staging/twitter/" + args(0) // staging/twitter/searches/opensource/2014/12/*
    val title = args(0) // 
    val tweets = sqlContext.jsonFile(filename)

    tweets.registerTempTable("tweets")

    val t = sqlContext.sql("SELECT distinct createdAt, user.name, id, text FROM tweets")

    var ft = new java.text.SimpleDateFormat("yyyy-MM-dd");

    var extractedTweets = t.map(t0 => (ft.format(new java.util.Date(t0(0).asInstanceOf[Long])), t0(1), t0(2), t0(3))).distinct()

    var words = t.flatMap(t0 => "[\n\t.;:?]".r.replaceAllIn(t0(3).toString, " ").toLowerCase().split(" "))
    var topWords = words.map(t0 => (t0,1)).reduceByKey(_ + _).map(t0 => (t0._2,t0._1)).sortByKey(ascending=false).take(10).map(t0 => (t0._2,t0._1))
    var topHashTags = words.filter(word => word.startsWith("#")).map(t0 => (t0,1)).reduceByKey(_ + _).map(t0 => (t0._2,t0._1)).sortByKey(ascending=false).take(10).map(t0 => (t0._2,t0._1))

    println(s"<html><head><title>$title</title></head>")
    println("<body>")
    println(s"<h1>$title</h1>")
    println("<h2>Tweets</h2>")
    extractedTweets.collect().foreach(tweet2Html)
    println("<h2>Stats</h2>")
    println("<h3>Tweets count</h3>")
    t.map(t0 => (ft.format(new java.util.Date(t0(0).asInstanceOf[Long])), 1)).reduceByKey(_ + _).sortByKey().collect().foreach(println)
    println("<h3>Top hashtags</h3>")
    topHashTags.foreach(println)
    println("<h3>Top words</h3>")
    topWords.foreach(println)

    println("</body>")
    println("<html>")
    sc.stop()
  }
}


