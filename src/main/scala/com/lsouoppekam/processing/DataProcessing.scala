package com.lsouoppekam.processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.broadcast.Broadcast
import com.lsouoppekam.utils._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.logging.log4j._

object DataProcessing {

  implicit val logger: Logger = LogManager.getLogger(getClass.getName)

  /**
    * This main processing method receives in input the input file the output path and
    * the broadcast blacklist dataFrame and returns for each domain/subdomain in the
    * input file, the top 25 pages per page view, ordered bu domain and countViews.
    * The method returns true if the processing was successful and false if the input
    * past does not exist or if the job was already done for the specified day and hour
    * @param inputFile Input of the file to be processed
    * @param outputPath Output directory to write the result
    * @param blackList Blacklist in a broadcast dataFrame
    * @param spark SparkSession
    * @return Boolean
    */
  def process(
      inputFile: String,
      outputPath: String,
      blackList: Broadcast[DataFrame]
  )(implicit spark: SparkSession): Boolean = {

    logger.info("check if the input file exists: " + inputFile)

    // check if the input file exists to avoid making the application crash
    if (!checkPath(inputFile)) {
      logger.warn("The input file does not exist, skipping: " + inputFile)
      return false
    }

    logger.info(
      "check if the work has already been done for the given day and hour: " + inputFile
    )

    // check if the work has already been done for the given day and hour
    if (checkPath(outputPath, "_SUCCESS")) {
      logger.warn(
        "The work was already done for the file - skipping: " + inputFile
      )
      return false
    }

    // read input
    val pageViews = Reader.readPages(inputFile)(spark)

    // Remove the blacklisted, agg and get the to 25
    val aggregatedPageViewsDF = aggregatePageViewsByDomain(pageViews, blackList)

    // write output
    Writer.saveAsCSV(aggregatedPageViewsDF, outputPath)

    true
  }

  /**
    * The method receives in input 2 dataFrames: pageViews and blackList and remove from
    * the pageViews all the blacklisted pages, and return a cleaned dataFrame
    * @param df Data Frame
    * @param blackList Blacklist pages in a broadcast data frame
    * @param spark SparkSession
    * @return DataFrame
    */
  def aggregatePageViewsByDomain(
      df: DataFrame,
      blackList: Broadcast[DataFrame]
  )(implicit spark: SparkSession): DataFrame = {

    val blackListDF = blackList.value

    logger.info("Remove the black listed pages")

    // remove the blacklisted pages with a left anti join
    // the goal is to remain with the pages having no match in the blacklist
    val cleanPagesDF = df.join(
      blackListDF,
      df("domainCode") === blackListDF("domain") && df(
        "pageTitle"
      ) === blackListDF("page"),
      "leftanti"
    )

    // aggregate the pages by domain and page and sum the countViews
    // This is a precaution just in case they are the information about
    // the count was split in a file. This is to regroup them
    val aggPageViews = cleanPagesDF
      .groupBy("domainCode", "pageTitle")
      .sum("countViews")

    //rank the results and Take the top 25 pages.
    val windowSpec =
      Window
        .partitionBy("domainCode")
        .orderBy(col("sum(countViews)").desc)

    aggPageViews
      .withColumn("rank", rank().over(windowSpec))
      .filter(col("rank") < 26)
      .orderBy(col("domainCode"), col("sum(countViews)").desc)

  }

  /**
    * The method checks if a file or directory exists
    * The method checks if the combination of path and fileName exists
    * and returns a boolean
    * @param path path to be checked
    * @param fileName a file name to be added to the path
    * @param spark SparkSession
    * @return Boolean
    */
  def checkPath(path: String, fileName: String = "")(implicit
      spark: SparkSession
  ): Boolean = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(new Path(path + "/" + fileName)))
      true
    else {
      false
    }
  }
}
