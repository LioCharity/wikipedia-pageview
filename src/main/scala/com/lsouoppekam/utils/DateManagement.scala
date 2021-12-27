package com.lsouoppekam.utils

import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter

object DateManagement {

  val dateFormat = "yyyy-MM-dd"
  val timeFormat = "HH:mm"

  /**
    * The method says is a string date is in the format yyyy-MM-dd
    * @param strDate date as string
    * @return Boolean
    */
  def dateValidator(strDate: String): Boolean = {
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    try {
      LocalDate.parse(strDate, formatter)
    } catch {
      case _: Throwable => return false
    }
    true
  }

  /**
    * The method says if a string time is in the format HH:mm
    * @param strTime time as a string
    * @return Boolean
    */
  def timeValidator(strTime: String): Boolean = {
    val formatter = DateTimeFormatter.ofPattern(timeFormat)
    try {
      LocalTime.parse(strTime, formatter)
    } catch {
      case _: Throwable => return false
    }
    true
  }

  /**
    * From string to date in format yyyy-MM-dd
    * @param strDate date as a string
    * @return LocalDate
    */
  def fromStringToDate(strDate: String): LocalDate = {
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    LocalDate.parse(strDate, formatter)
  }

  /**
    * From string to time in format HH:mm
    * @param strTime time as a string
    * @return LocalTime
    */
  def fromStringToTime(strTime: String): LocalTime = {
    val formatter = DateTimeFormatter.ofPattern(timeFormat)
    LocalTime.parse(strTime, formatter)
  }

  /**
    * For a given start and end, the method generates all the hours in the range
    * @param theStart LocalDate
    * @param theEnd LocalDate
    * @return List[LocalTime]
    */
  def generateTimes(theStart: LocalTime, theEnd: LocalTime): List[LocalTime] = {
    if (theStart.isAfter(theEnd)) {
      throw new Exception("The start time should be lesser than the end time")
    }
    var times = List[LocalTime]()
    var tmp = theStart
    while (!tmp.equals(theEnd)) {
      times = times :+ tmp
      tmp = tmp.plusHours(1)
    }
    times :+ tmp
  }

  /**
    * For a given start and end, the method generates all the date in the range
    * @param startDate LocalDate
    * @param endDate LocalDate
    * @return List[LocalDate]
    */
  def generateDates(
      startDate: LocalDate,
      endDate: LocalDate
  ): List[LocalDate] = {
    if (startDate.isAfter(endDate)) {
      throw new Exception("the start date should be lesser than the end date")
    }
    val dates: IndexedSeq[LocalDate] =
      (0L to (endDate.toEpochDay - startDate.toEpochDay))
        .map(days => startDate.plusDays(days))
    dates.toList
  }

}
