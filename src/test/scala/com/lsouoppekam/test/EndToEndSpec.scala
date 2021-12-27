package com.lsouoppekam.test

import org.specs2.mutable.Specification
import org.apache.spark.sql.SparkSession
import com.lsouoppekam.MainApp
import com.lsouoppekam.processing._
import org.apache.hadoop.fs.{FileSystem, Path}

class EndToEndSpec extends Specification {

  val spark: SparkSession =
    SparkSession.builder().config("spark.master", "local").getOrCreate()
  val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  "End to end run" should {
    "Range - Date and range of time" in {

      val output = "src/test/resources/pathToCheck"
      val arguments = Array(
        "-r",
        "true",
        "-s",
        "2021-02-01",
        "-h",
        "00:00",
        "-j",
        "02:00",
        "-b",
        "src/test/resources/blacklist/blacklist_domains_and_pages",
        "-i",
        "src/test/resources",
        "-o",
        output
      )
      MainApp.main(arguments)

      DataProcessing.checkPath(output + "/2021/2021-02/01/0000/", "_SUCCESS")(
        spark
      ) shouldEqual true

      DataProcessing.checkPath(output + "/2021/2021-02/01/0100/", "_SUCCESS")(
        spark
      ) shouldEqual true

      DataProcessing.checkPath(output + "/2021/2021-02/01/0200/", "_SUCCESS")(
        spark
      ) shouldEqual true

      // delete the files written
      fs.delete(new Path(output + "/2021/2021-02/01"), true)
    }
  }
}
