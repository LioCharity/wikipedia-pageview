package com.lsouoppekam

import org.apache.spark.sql.SparkSession
import com.lsouoppekam.utils.Reader
import com.lsouoppekam.processing._
import org.apache.logging.log4j._
import com.lsouoppekam.utils._

object MainApp {

  implicit val logger: Logger = LogManager.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info(
      "Parse command line argument. Arguments: " + args.mkString(" - ")
    )
    val commandLineArguments =
      ArgumentsParser.init().parse(args, ArgumentsParser.Arguments())

    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .config("spark.master", commandLineArguments.get.master)
        .appName("Top Page Views")
        .getOrCreate()

    try {

      /**
        * Get the path to read the black listed pages.
        * A default value is provided with a version of the black listed pages embedded
        * in the code
        */
      val pathToBlacklist =
        if (commandLineArguments.get.pathToBlacklist != "")
          commandLineArguments.get.pathToBlacklist
        else
          Constants.pathToBlackListDefault

      logger.info("Read the black list data at: " + pathToBlacklist)

      /**
        * Read the black listed pages and broadcast to all the workers.
        */
      val blackList =
        spark.sparkContext.broadcast(
          Reader.readBlackList(pathToBlacklist)(spark)
        )

      logger.info(
        "Generate input data files. isRange=" + commandLineArguments.get.isRange.toString
      )

      /**
        * According to if it is a range processing or not, generate all the files to be taken
        * into account during the processing.
        * For each input:
        *  - Read the files
        *  - Process
        *  - Write down in a single CSV file
        */
      val dataFiles = if (!commandLineArguments.get.isRange) {
        Reader.buildPathsNoRanges(commandLineArguments.get)
      } else Reader.buildPathsRanges(commandLineArguments.get)

      logger.info(
        "Process data files. Inputs/Output to process: " + dataFiles
          .mkString(" -- ")
      )
      dataFiles.foreach(x => DataProcessing.process(x._1, x._2, blackList))

    } finally {
      spark.stop()
    }
  }
}
