package com.lsouoppekam

import scopt.OptionParser
import com.lsouoppekam.utils.DateManagement.{dateValidator, timeValidator}

/**
  * command line parser.
  * The default values are in the case class Arguments
  */
object ArgumentsParser {

  case class Arguments(
      var inputPath: String = "",
      var startDate: String = "",
      var startHour: String = "",
      var endDate: String = "",
      var endHour: String = "",
      var outputPath: String = "",
      var pathToBlacklist: String = "",
      var isRange: Boolean = false,
      var master: String = "local"
  )

  def init(): OptionParser[Arguments] = {
    new OptionParser[Arguments]("Arguments") {

      head("Command Line Argument Parser with Scopt")

      override def errorOnUnknownArgument = true

      opt[String]('i', "inputPath")
        .required()
        .action((x, c) => c.copy(inputPath = x))
        .text("input path")

      opt[String]('s', "startDate")
        .optional()
        .action((x, c) => c.copy(startDate = x))
        .validate(x =>
          if (dateValidator(x)) success
          else
            failure("Option -s or --startDate must have the format yyyy-MM-dd")
        )
        .text("start date")

      opt[String]('h', "startHour")
        .optional()
        .validate(x =>
          if (timeValidator(x)) success
          else
            failure("Option -h or --startHour must have the format HH:mm")
        )
        .action((x, c) => c.copy(startHour = x))
        .text("start hour")

      opt[String]('e', "endDate")
        .optional()
        .validate(x =>
          if (dateValidator(x)) success
          else
            failure("Option -e or --endDate must have the format yyyy-MM-dd")
        )
        .action((x, c) => c.copy(endDate = x))
        .text("end date")

      opt[String]('j', "endHour")
        .optional()
        .validate(x =>
          if (timeValidator(x)) success
          else
            failure("Option -j or --endHour must have the format HH:mm")
        )
        .action((x, c) => c.copy(endHour = x))
        .text("end hour")

      opt[String]('o', "outputPath")
        .required()
        .action((x, c) => c.copy(outputPath = x))
        .text("output path")

      opt[String]('b', "pathToBlacklist")
        .optional()
        .action((x, c) => c.copy(pathToBlacklist = x))
        .text("path to black list")

      opt[Boolean]('r', "isRange")
        .optional()
        .action((x, c) => c.copy(isRange = x))
        .text(
          "Specified if it is a range processing or not. The default is false"
        )

      opt[String]('m', "master")
        .optional()
        .action((x, c) => c.copy(master = x))
        .text("spark master")
    }
  }
}
