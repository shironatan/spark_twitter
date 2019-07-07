import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkConf
import java.util.regex._
import org.atilika.kuromoji._
import org.atilika.kuromoji.Tokenizer._

System.setProperty("twitter4j.oauth.consumerKey","***")
System.setProperty("twitter4j.oauth.consumerSecret","***")
System.setProperty("twitter4j.oauth.accessToken","***")
System.setProperty("twitter4j.oauth.accessTokenSecret","***")

val ssc = new StreamingContext(sc,Seconds(60))
val stream = TwitterUtils.createStream(ssc,None,Array("****serch word****"))

val tweetStream = stream.flatMap(status =>{
    val tokenizer : Tokenizer = Tokenizer.builder().build()
    val features : scala.collection.mutable.ArrayBuffer[String] = new collection.mutable.ArrayBuffer[String]()
    var tweetText : String = status.getText()

    val japanese_pattern : Pattern = Pattern.compile("[¥¥u3040-¥¥u309F]+")
    if(japanese_pattern.matcher(tweetText).find()){
        tweetText = tweetText.replaceAll("http(s*)://(.*)/","").replaceAll("¥¥uff57","")
        val tokens : java.util.List[Token] = tokenizer.tokenize(tweetText)
        val pattern : Pattern = Pattern.compile("^[a-zA-Z]+$|^[0-9]+$")
        for(index <- 0 to tokens.size()-1){
            val token = tokens.get(index)
            val matcher : Matcher = pattern.matcher(token.getSurfaceForm())
            if(token.getSurfaceForm().length() >= 3 && !matcher.find()){
                features += (token.getSurfaceForm() + "-" + token.getAllFeatures())
            }
        }
    }
    (features)
})

val topCounts60 = tweetStream.map((_, 1)
).reduceByKeyAndWindow(_+_, Seconds(60*60)
).map{case (topic,count) => (count,topic)
}.transform(_.sortByKey(false))
topCounts60.foreachRDD(rdd => {
    val topList = rdd.take(20)
    println("Popular topics in last 60*60 seconds (%s words):".format(rdd.count()))
    topList.foreach{case (count,tag) => println("%s (%s tweets)".format(tag,count))}
})

ssc.start()
ssc.awaitTermination()

