// Laurian Duma - s1091563

package org.rubigdata

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.NullWritable
import de.l3s.concatgz.io.warc.{WarcGzInputFormat, WarcWritable}
import de.l3s.concatgz.data.WarcRecord
import org.apache.spark.SparkConf
import scala.collection.JavaConverters._
import org.jsoup.Jsoup

object RUBigDataApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("RUBigDataApp")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[WarcRecord]))
      .set("spark.driver.memory", "3g")
      .set("spark.executor.memory", "5g")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "4g")

    implicit val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext
    val warcFilePath = (0 to 2).map(a => f"hdfs:/single-warc-segment/CC-MAIN-20210410105831-20210410135831-$a%05d.warc.gz")

    val warcs = sc.newAPIHadoopFile(
      warcFilePath.mkString(","),
      classOf[WarcGzInputFormat],
      classOf[NullWritable],
      classOf[WarcWritable]
    )
      
    val environmentalImpactAnalysis = warcs.map { case (_, warcRecord) => warcRecord.getRecord() }
      .filter(record => record.getHeader.getUrl != null && record.getHttpStringBody != null)
      .map { record =>
        val document = Jsoup.parse(record.getHttpStringBody)
        val bodyText = document.select("body").text()
        val totalWordCount = bodyText.split("\\s+").length
        
        val environmentalKeywordsPattern = "\\b(([Cc]limate [Cc]hange)|([Gg]lobal [Ww]arming)|([Pp]ollution)|([Rr]enewable [Ee]nergy)|([Ss]ustainability)|([Rr]ecycling)|([Cc]limate [Aa]ction)|([Ee]co-friendly)|([Cc]arbon [Ee]missions)|([Gg]reenhouse [Gg]ases)|([Ss]olar [Pp]ower)|([Ww]ind [Pp]ower)|([Ff]ossil [Pp]ower))\\b".r
        
        val keywordMatchCount = environmentalKeywordsPattern.findAllIn(bodyText).size
        val relevanceRatio = if (totalWordCount > 0) keywordMatchCount.toDouble / totalWordCount else 0.0
        (record.getHeader.getUrl, keywordMatchCount, relevanceRatio)
      }
      .filter(_._3 > 0)

    val df = environmentalImpactAnalysis.toDF("url", "pattern_count", "standardized_wordcount")
    df.createOrReplaceTempView("df")

    val results = spark.sql("SELECT url, pattern_count, standardized_wordcount FROM df ORDER BY pattern_count DESC LIMIT 5").collect()

    println("\n########## RESULTS ##########")
    println("Top 5 webpages in Warc-files mentioning environmental related terms (standardized by total word count):")
    results.foreach(println)
    println("########### END RESULTS ############\n")

    spark.stop()
  }
}

