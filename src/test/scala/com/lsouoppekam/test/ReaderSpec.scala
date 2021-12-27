package com.lsouoppekam.test

import java.time.LocalDateTime

import com.lsouoppekam.ArgumentsParser
import org.specs2.mutable.Specification
import org.apache.spark.sql.SparkSession
import com.lsouoppekam.utils.Reader.{
  buildPathsNoRanges,
  buildPathsRanges,
  readBlackList
}

class ReaderSpec extends Specification {

  val spark: SparkSession =
    SparkSession.builder().config("spark.master", "local").getOrCreate()

  "Reader black listed" should {
    "black listed" in {
      val path = "src/test/resources/blacklist/blacklist_domains_and_pages"
      val df = readBlackList(path)(spark)
      df.show()
      df.count() shouldEqual 2
    }
  }

  "Path builder No ranges" should {
    "date and time" in {
      val arguments =
        ArgumentsParser
          .init()
          .parse(
            Array(
              "-s",
              "2021-01-01",
              "-h",
              "00:00",
              "-i",
              "/input/path",
              "-o",
              "/output/path"
            ),
            ArgumentsParser.Arguments()
          )
      val paths = buildPathsNoRanges(arguments.get)

      val expectedPaths = List(
        (
          "/input/path/2021/2021-01/pageviews-20210101-000000.gz",
          "/output/path/2021/2021-01/01/0000"
        )
      )
      paths shouldEqual expectedPaths
    }

    "date and guess the time - first hour of the day" in {
      val arguments =
        ArgumentsParser
          .init()
          .parse(
            Array(
              "-s",
              "2021-01-01",
              "-i",
              "/input/path",
              "-o",
              "/output/path"
            ),
            ArgumentsParser.Arguments()
          )
      val paths = buildPathsNoRanges(arguments.get)

      val expectedPaths = List(
        (
          "/input/path/2021/2021-01/pageviews-20210101-000000.gz",
          "/output/path/2021/2021-01/01/0000"
        )
      )
      paths shouldEqual expectedPaths
    }

    "guess date and time - previous day" in {
      val arguments =
        ArgumentsParser
          .init()
          .parse(
            Array(
              "-i",
              "/input/path",
              "-o",
              "/output/path"
            ),
            ArgumentsParser.Arguments()
          )
      val paths = buildPathsNoRanges(arguments.get)

      val dateAndTime = LocalDateTime.now().minusDays(1).toString.split("T")
      val t = dateAndTime(0).split("-")
      val year = t(0)
      val yearMonth = t(0) + "-" + t(1)
      val day = t(2)
      val aTimeZeroed = dateAndTime(1).split(":")(0) + "00"
      val expectedPaths = List(
        (
          "/input/path/" + year + "/" + yearMonth + "/pageviews-" + dateAndTime(
            0
          ).replace("-", "") + "-" + aTimeZeroed + "00.gz",
          "/output/path/" + year + "/" + yearMonth + "/" + day + "/" + aTimeZeroed
        )
      )
      print("PATHS:", paths)
      print("EXPECTED PATHS:", expectedPaths)
      paths shouldEqual expectedPaths
    }

  }

  "path builder - ranges" should {
    "No date" in {
      val arguments =
        ArgumentsParser
          .init()
          .parse(
            Array(
              "-i",
              "/input/path",
              "-o",
              "/output/path"
            ),
            ArgumentsParser.Arguments()
          )
      val paths = buildPathsRanges(arguments.get)

      val dateAndTime = LocalDateTime.now().minusDays(1).toString.split("T")
      val t = dateAndTime(0).split("-")
      val year = t(0)
      val yearMonth = t(0) + "-" + t(1)
      val day = t(2)
      val aTimeZeroed = "0000"

      val expectedTop = (
        "/input/path/" + year + "/" + yearMonth + "/pageviews-" + dateAndTime(
          0
        ).replace("-", "") + "-" + aTimeZeroed + "00.gz",
        "/output/path/" + year + "/" + yearMonth + "/" + day + "/" + aTimeZeroed
      )
      paths.head shouldEqual expectedTop
    }

    "A date and a range" in {
      val arguments =
        ArgumentsParser
          .init()
          .parse(
            Array(
              "-s",
              "2021-12-23",
              "-h",
              "12:00",
              "-j",
              "13:00",
              "-i",
              "/input/path",
              "-o",
              "/output/path"
            ),
            ArgumentsParser.Arguments()
          )
      val paths = buildPathsRanges(arguments.get)

      val expected = List(
        (
          "/input/path/2021/2021-12/pageviews-20211223-120000.gz",
          "/output/path/2021/2021-12/23/1200"
        ),
        (
          "/input/path/2021/2021-12/pageviews-20211223-130000.gz",
          "/output/path/2021/2021-12/23/1300"
        )
      )
      paths shouldEqual expected
    }

    "Start date and end date" in {
      val arguments =
        ArgumentsParser
          .init()
          .parse(
            Array(
              "-s",
              "2021-12-23",
              "-e",
              "2021-12-24",
              "-i",
              "/input/path",
              "-o",
              "/output/path"
            ),
            ArgumentsParser.Arguments()
          )
      val paths = buildPathsRanges(arguments.get)
      paths.length shouldEqual 48
    }

  }

}
