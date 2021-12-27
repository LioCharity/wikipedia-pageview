package com.lsouoppekam.bom

/**
  * Case class used to format the wiki page views
  * @param domainCode domaine code
  * @param pageTitle wiki page
  * @param countViews count of the page view
  * @param totalResponseSize size of reponse
  */
case class Page(
    domainCode: String,
    pageTitle: String,
    countViews: Long,
    totalResponseSize: Long
)
