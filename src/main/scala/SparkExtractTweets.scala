import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._

import java.text.SimpleDateFormat
import java.text.DateFormat
import java.util.Date

import java.io.PrintWriter

object SparkExtractTweets {

  def cleanText(text: Any) : String = {
    val t1 = "[\n\t]".r.replaceAllIn(text.toString, " ")
    val t2 = "[:?!.]$".r.replaceAllIn(t1, "")
    return t2;
  }

  def formatTweet(row:(Any,Any,Any,Any)) : String = {
    val (createdAt, user, id, text) = row
    return s"$createdAt\t$user\t$id\t$text";
  }

  def formatTopRecord(row:(Any,Any)) : String = {
    val (text, count) = row
    return s"$text\t$count";
  }

  def saveArrayToFile(array: Array[String], filename: String) {
    val s = new PrintWriter(filename)
    array.foreach(s.println)
    s.close()
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Twitter-extractTweets")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val source_path = args(0)
    val target_path = args(1)

    val tweets = sqlContext.jsonFile(source_path)
    tweets.registerTempTable("tweets")

    val t = sqlContext.sql("SELECT distinct createdAt, user.screenName, id, text FROM tweets")

    var ft = new java.text.SimpleDateFormat("yyyy-MM-dd");

    var extractedTweets = t.map(t0 => (ft.format(new java.util.Date(t0(0).asInstanceOf[Long])), t0(1), t0(2), cleanText(t0(3)))).distinct()

    var words = t.flatMap(t0 => "[\n\t]".r.replaceAllIn(t0(3).toString, " ").toLowerCase().split(" "))
    var topHashTags = words.filter(word => word.startsWith("#")).map(t0 => (cleanText(t0),1)).reduceByKey(_ + _).map(t0 => (t0._2,t0._1)).sortByKey(ascending=false).take(10).map(t0 => (t0._2,t0._1))
    var topMentions = words.filter(word => word.startsWith("@")).map(t0 => (cleanText(t0),1)).reduceByKey(_ + _).map(t0 => (t0._2,t0._1)).sortByKey(ascending=false).take(10).map(t0 => (t0._2,t0._1))

    var topUsers = t.map(t0 => (cleanText(t0(1)),1)).reduceByKey(_ + _).map(t0 => (t0._2,t0._1)).sortByKey(ascending=false).take(10).map(t0 => (t0._2,t0._1))

    // saving tweets
    
    saveArrayToFile(extractedTweets.map(t0 => formatTweet(t0)).collect(), target_path + "/tweets.tsv")

    saveArrayToFile(topHashTags.map(r => formatTopRecord(r)), target_path + "/top-hashtags.tsv")
    saveArrayToFile(topMentions.map(r => formatTopRecord(r)), target_path + "/top-mentions.tsv")
    saveArrayToFile(topUsers.map(r => formatTopRecord(r)), target_path + "/top-users.tsv")

    println("<h2>Stats</h2>")
    println("<h3>Tweets count</h3>")
    t.map(t0 => (ft.format(new java.util.Date(t0(0).asInstanceOf[Long])), 1)).reduceByKey(_ + _).sortByKey().collect().foreach(println)

    sc.stop()
  }
}


