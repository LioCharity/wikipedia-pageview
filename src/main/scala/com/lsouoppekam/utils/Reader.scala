package com.lsouoppekam.utils

import com.lsouoppekam.ArgumentsParser.Arguments
import com.lsouoppekam.bom.Blacked
import com.lsouoppekam.bom.Page
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.{LocalDate, LocalDateTime, LocalTime}
import com.lsouoppekam.utils.DateManagement._
import org.apache.logging.log4j._

object Reader {

  implicit val logger: Logger = LogManager.getLogger(getClass.getName)

  /**
    * The method reads for a given file, a page view.
    * The method reads the file as a text file and split a line in 4
    * the build the object Page and returns a dataFrame with the schema
    * like define in the case class Page
    * @param filePath file of the path to be read
    * @param spark sparkSession
    * @return DataFrame
    */
  def readPages(filePath: String)(implicit
      spark: SparkSession
  ): DataFrame = {

    logger.info("Read page view: " + filePath)

    val rdd = spark.sparkContext.textFile(filePath).map { x =>
      val l = x.split(" ")
      Page(l(0), l(1), l(2).toLong, l(3).toLong)
    }
    val df = spark.createDataFrame(rdd)
    df
  }

  /**
    * the method reads the file containing the black listed page and returns a dataFrame
    * with the schema of the case class Blacked
    * @param path path to the blacklist file
    * @param spark SparkSession
    * @return DataFrame
    */
  def readBlackList(path: String)(implicit spark: SparkSession): DataFrame = {

    logger.info("Read the blacklisted page: " + path)

    val rdd =
      spark.sparkContext
        .textFile(path)
        .map { x =>
          val l = x.split(" ")
          Blacked(l(0), l(1))
        }
    val df = spark.createDataFrame(rdd)
    df
  }

  /**
    * The method generates a couple of (inputFile, outputDir) according to the command line parameters
    * The method is called when the user is not requesting to process a range of dates or hours
    * - If no date and time is provided, the default is 24h earlier
    * - If only a date is given, the default time is 00:00
    * @param arguments Argument Class
    * @return List[(String, String)]. For this no range method, the size of the list is 1
    */
  def buildPathsNoRanges(arguments: Arguments): List[(String, String)] = {
    val rootInput = arguments.inputPath
    val rootOutput = arguments.outputPath
    val startDate = arguments.startDate
    val startTime = arguments.startHour
    var paths = List[(String, String)]()

    if (startDate == "") {

      /**
        * In this case I guess the date and the time which is 24h earlier
        */
      val dateAndTime = LocalDateTime.now().minusDays(1).toString.split("T")
      val aTimeZeroed = dateAndTime(1).split(":")(0) + ":00"
      val (inputFilePath, outputPath) =
        inputOutputPair(rootInput, rootOutput, dateAndTime(0), aTimeZeroed)
      paths = paths :+ (inputFilePath, outputPath)
      paths
    } else {
      if (startTime == "") {

        /**
          * In this case I have a date but not a time so 00:00 is the default time
          */
        val (inputFilePath, outputPath) =
          inputOutputPair(rootInput, rootOutput, startDate, "00:00")
        paths = paths :+ (inputFilePath, outputPath)
        paths

      } else {

        /**
          * Here I have the date and the time so I generate the input file and the output directory
          */
        val (inputFilePath, outputPath) =
          inputOutputPair(rootInput, rootOutput, startDate, startTime)
        paths = paths :+ (inputFilePath, outputPath)
        paths

      }
    }
  }

  /**
    * The method is called when it's about processing a range of dates or times.
    * According to the specified range, it generates all the path belonging to the range
    * @param arguments Arguments class with command line arguments
    * @return List[(String, String)]
    */
  def buildPathsRanges(arguments: Arguments): List[(String, String)] = {
    val rootInput = arguments.inputPath
    val rootOutput = arguments.outputPath
    val startDate = arguments.startDate
    val endDate = arguments.endDate
    val startTime = arguments.startHour
    val endTime = arguments.endHour

    if (startDate == "") {

      /**
        * If the start date is not provided, the yesterday date is taken and all the hours of that day are
        * considered
        */
      val yesterdayDate = LocalDate.now().minusDays(1)
      val hours =
        DateManagement.generateTimes(LocalTime.of(0, 0), LocalTime.of(23, 0))
      return hours.map(h =>
        inputOutputPair(
          rootInput,
          rootOutput,
          yesterdayDate.toString,
          h.toString.split(":")(0) + "00"
        )
      )
    }

    if (startDate != "" && endDate == "" && startTime == "" && endTime == "") {

      /**
        * Uf only the start date is provided, I consider all the hours of the day
        */
      val hours =
        DateManagement.generateTimes(LocalTime.of(0, 0), LocalTime.of(23, 0))
      return hours.map(h =>
        inputOutputPair(
          rootInput,
          rootOutput,
          startDate,
          h.toString.split(":")(0) + "00"
        )
      )
    }

    if (startDate != "" && startTime != "" && endTime != "" && endDate == "") {

      /**
        * The start date is provided with a range of hours
        */
      val hours =
        DateManagement.generateTimes(
          fromStringToTime(startTime),
          fromStringToTime(endTime)
        )
      return hours.map(h =>
        inputOutputPair(
          rootInput,
          rootOutput,
          startDate,
          h.toString.split(":")(0) + "00"
        )
      )
    }

    if (startDate != "" && endDate != "" && startTime == "" && endTime == "") {

      /**
        * If a range of date is provided but not a range of hours, I consider the whole day for
        * all the date belonging to the range
        */
      val dates =
        generateDates(fromStringToDate(startDate), fromStringToDate(endDate))
      val hours =
        DateManagement.generateTimes(LocalTime.of(0, 0), LocalTime.of(23, 0))
      val allPaths = for {
        date <- dates
        hour <- hours
      } yield {
        inputOutputPair(
          rootInput,
          rootOutput,
          date.toString,
          hour.toString.split(":")(0) + "00"
        )
      }
      return allPaths
    }

    if (startDate != "" && endDate != "" && startTime != "" && endTime != "") {

      /**
        * range of dates and range of hours
        */
      val dates =
        generateDates(fromStringToDate(startDate), fromStringToDate(endDate))
      val hours =
        DateManagement.generateTimes(
          fromStringToTime(startTime),
          fromStringToTime(endTime)
        )
      val allPaths = for {
        date <- dates
        hour <- hours
      } yield {
        inputOutputPair(
          rootInput,
          rootOutput,
          date.toString,
          hour.toString.split(":")(0) + "00"
        )
      }
      return allPaths
    }

    // if the scenario is not covered, an exception will be throw
    throw new Exception("Wrong set of parameters. Refer to the user guide")
  }

  /**
    * The method generate the file name and the output dir concatenating the parameters given
    * @param rootInput String rootInput
    * @param rootOutput String rootOutput
    * @param aDate String date
    * @param aTime String time
    * @return
    */
  def inputOutputPair(
      rootInput: String,
      rootOutput: String,
      aDate: String,
      aTime: String
  ): (String, String) = {
    val tmp = aDate.split("-")
    val yearMontPath = tmp(0) + "/" + tmp(0) + "-" + tmp(1)
    val inputFilePath =
      rootInput + "/" + yearMontPath + "/" + "pageviews-" + aDate
        .replace("-", "") + "-" + aTime
        .replace(":", "") + "00.gz"

    val outputPath =
      rootOutput + "/" + yearMontPath + "/" + tmp(2) + "/" + aTime
        .replace(":", "")
    (inputFilePath, outputPath)
  }
}
