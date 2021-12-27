name := "wikipedia-pageview"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2" % "provided"

libraryDependencies += "org.specs2" %% "specs2-core" % "4.13.1" % Test

libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.17.0"

fork in Test := true

parallelExecution in Test := false
