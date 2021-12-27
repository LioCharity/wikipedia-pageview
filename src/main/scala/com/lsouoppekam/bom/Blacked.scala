package com.lsouoppekam.bom

/**
  * Case class used to format the blacklist data
  * @param domain Domain code
  * @param page wiki page
  */
case class Blacked(
    domain: String,
    page: String
)
