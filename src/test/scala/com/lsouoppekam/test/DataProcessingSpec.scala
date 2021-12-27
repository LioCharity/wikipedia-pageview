package com.lsouoppekam.test

import org.apache.spark.sql.SparkSession
import org.specs2.mutable.Specification
import org.apache.hadoop.fs.{FileSystem, Path}
import com.lsouoppekam.processing.DataProcessing._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

class DataProcessingSpec extends Specification {

  val spark: SparkSession =
    SparkSession.builder().config("spark.master", "local").getOrCreate()
  val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  val blackList = List(
    Row("aa", "paris")
  )
  val blackListSchema: StructType = StructType(
    List(
      StructField("domain", StringType, nullable = true),
      StructField("page", StringType, nullable = true)
    )
  )
  val rdd2: RDD[Row] = spark.sparkContext.parallelize(blackList)
  val blackListDF: DataFrame = spark.createDataFrame(rdd2, blackListSchema)

  "input and output checks - check path" should {
    "_SUCCESS file does exist" in {
      val res =
        checkPath("src/test/resources/pathToCheck/dummyDir2", "_SUCCESS")(spark)
      res shouldEqual true
    }
    "_SUCCESS file does not exist" in {
      val res =
        checkPath("src/test/resources/pathToCheck/dummyDir", "_SUCCESS")(spark)
      res shouldEqual false
    }
  }

  "Data aggregation" should {
    "aggregatePageViewsByDomain" in {
      val pageviews = List(
        Row("aa", "Main_Page", 1L, 1L),
        Row("aa", "Main_Page", 1L, 1L),
        Row("aa", "paris", 1L, 2L),
        Row("aa", "londres", 3L, 23L),
        Row("aa.b", "Main_Page", 2L, 1L),
        Row("aa.b", "milano", 3L, 1L),
        Row("aa.m", "Main_Page", 4L, 1L),
        Row("aa.l", "Main_Page", 2L, 4L),
        Row("ee", "Main_Page", 2L, 1L),
        Row("ee", "dummy", 3L, 2L),
        Row("ee", "rich", 7L, 2L),
        Row("ee", "sober", 1L, 2L),
        Row("ee.q", "Main_Page", 1L, 15L)
      )
      val pageSchema = StructType(
        List(
          StructField("domainCode", StringType, nullable = true),
          StructField("pageTitle", StringType, nullable = true),
          StructField("countViews", LongType, nullable = true),
          StructField("totalResponseSize", LongType, nullable = true)
        )
      )
      val rdd = spark.sparkContext.parallelize(pageviews)
      val pageviewsDF = spark.createDataFrame(rdd, pageSchema)

      val aggPageViews = aggregatePageViewsByDomain(
        pageviewsDF,
        spark.sparkContext.broadcast(blackListDF)
      )(spark)

      aggPageViews.show()

      val expectedResult = List(
        List("aa", "londres", 3, 1),
        List("aa", "Main_Page", 2, 2),
        List("aa.b", "milano", 3, 1),
        List("aa.b", "Main_Page", 2, 2),
        List("aa.l", "Main_Page", 2, 1),
        List("aa.m", "Main_Page", 4, 1),
        List("ee", "rich", 7, 1),
        List("ee", "dummy", 3, 2),
        List("ee", "Main_Page", 2, 3),
        List("ee", "sober", 1, 4),
        List("ee.q", "Main_Page", 1, 1)
      )
      val result = aggPageViews.collect().map(x => x.toSeq.toList).toList

      result shouldEqual expectedResult
    }
  }

  "Process method" should {
    "work already done for specified day and time" in {
      val inputFile =
        "src/test/resources/2021/2021-01/pageviews-20210101-000000.gz"
      val outputDir = "src/test/resources/pathToCheck/2021/2021-01/01/0000"
      val result = process(
        inputFile,
        outputDir,
        spark.sparkContext.broadcast(blackListDF)
      )(spark)
      result shouldEqual false
    }
    "successful process method" in {
      val inputFile =
        "src/test/resources/2021/2021-01/pageviews-20210101-010000.gz"
      val outputDir = "src/test/resources/pathToCheck/2021/2021-01/01/0100"
      val result = process(
        inputFile,
        outputDir,
        spark.sparkContext.broadcast(blackListDF)
      )(spark)

      result shouldEqual true

      // delete the files written
      fs.delete(new Path(outputDir), true)

    }

    "Process method - input file does not exist" in {
      val inputFile =
        "src/test/resources/2021/2021-01/pageviews-20210101-xxx.gz"
      val outputDir = "src/test/resources/pathToCheck/2021/2021-01/01/0100"
      val result = process(
        inputFile,
        outputDir,
        spark.sparkContext.broadcast(blackListDF)
      )(spark)

      result shouldEqual false
    }
  }
}
