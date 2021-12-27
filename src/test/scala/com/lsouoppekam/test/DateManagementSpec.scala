package com.lsouoppekam.test

import org.specs2.mutable.Specification
import com.lsouoppekam.utils.DateManagement
class DateManagementSpec extends Specification {

  "Date and hour validation" should {
    "Date validation - correct data" in {
      val res = DateManagement.dateValidator("2021-12-24")
      res shouldEqual true
    }
    "Date validation - incorrect data" in {
      val res = DateManagement.dateValidator("2021-15-24")
      res shouldEqual false
    }
    "Time validation - correct time" in {
      val res = DateManagement.timeValidator("14:00")
      res shouldEqual true
    }
    "Time validation - correct time" in {
      val res = DateManagement.timeValidator("14-00")
      res shouldEqual false
    }
    "Time validation - correct time 2" in {
      val res = DateManagement.timeValidator("1:00")
      res shouldEqual false
    }

  }
}
