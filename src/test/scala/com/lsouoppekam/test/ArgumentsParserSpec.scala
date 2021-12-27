package com.lsouoppekam.test

import com.lsouoppekam.ArgumentsParser
import com.lsouoppekam.ArgumentsParser.Arguments
import org.specs2.mutable.Specification

class ArgumentsParserSpec extends Specification {
  "Arguments parser" should {

    "case 1: Only required arguments" in {
      val res =
        ArgumentsParser
          .init()
          .parse(
            Array("-i", "/input/path", "-o", "/output/path"),
            ArgumentsParser.Arguments()
          )
      res shouldEqual Some(
        Arguments("/input/path", "", "", "", "", "/output/path", "")
      )
    }

    "case 2: Only start date provided" in {
      val res =
        ArgumentsParser
          .init()
          .parse(
            Array(
              "-i",
              "/input/path",
              "-o",
              "/output/path",
              "-s",
              "2021-12-23"
            ),
            ArgumentsParser.Arguments()
          )
      res shouldEqual Some(
        Arguments("/input/path", "2021-12-23", "", "", "", "/output/path", "")
      )
    }

    "case 3: Start date and end date" in {
      val res =
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
      res shouldEqual Some(
        Arguments(
          "/input/path",
          "2021-12-23",
          "",
          "2021-12-24",
          "",
          "/output/path",
          ""
        )
      )
    }

    "case 4: Start date start hour and end date" in {
      val res =
        ArgumentsParser
          .init()
          .parse(
            Array(
              "-s",
              "2021-12-23",
              "-h",
              "12:00",
              "-e",
              "2021-12-24",
              "-i",
              "/input/path",
              "-o",
              "/output/path"
            ),
            ArgumentsParser.Arguments()
          )
      res shouldEqual Some(
        Arguments(
          "/input/path",
          "2021-12-23",
          "12:00",
          "2021-12-24",
          "",
          "/output/path",
          ""
        )
      )
    }

    "case 5: all parameters" in {
      val res =
        ArgumentsParser
          .init()
          .parse(
            Array(
              "-s",
              "2021-12-23",
              "-h",
              "12:00",
              "-j",
              "12:00",
              "-e",
              "2021-12-24",
              "-i",
              "/input/path",
              "-o",
              "/output/path"
            ),
            ArgumentsParser.Arguments()
          )
      res shouldEqual Some(
        Arguments(
          "/input/path",
          "2021-12-23",
          "12:00",
          "2021-12-24",
          "12:00",
          "/output/path",
          ""
        )
      )
    }

  }

}
