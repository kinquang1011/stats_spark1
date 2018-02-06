name := "stats-spark"

assemblyJarName := "stats-spark.jar"

version := "1.0"

scalaVersion := "2.11.7"

unmanagedBase := baseDirectory.value / "unlib"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.2" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.7.1" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "1.6.2" % "provided",
  "commons-pool" % "commons-pool" % "1.5.5" % "provided",
  "net.liftweb" % "lift-json_2.11" % "2.6.3",
  "org.apache.spark" % "spark-hive_2.11" % "1.6.2" % "provided",
   "mysql" % "mysql-connector-java" % "5.1.38", 
  "org.apache.commons" % "commons-csv" % "1.1",
  "com.databricks" % "spark-csv_2.11" % "1.4.0"
)
val excludedJarsName = Seq(
		"paranamer-2.4.1.jar",
		"scala-parser-combinators_2.11-1.0.4.jar",
		"scala-reflect-2.11.7.jar",
		"scala-reflect-2.10.5.jar",
		"scala-xml_2.11-1.0.4.jar",
		"scala-compiler-2.11.7.jar",
		"scala-compiler-2.10.5.jar",
		"scalap-2.10.5.jar",
		"scalap-2.11.7.jar",
        "scala-library-2.10.5.jar",
        "scala-library-2.11.7.jar",
        "univocity-parsers-1.5.1.jar",
        "spark-assembly-1.6.0-hadoop2.6.0.jar"
)
excludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {
	c => excludedJarsName exists { c.data.getName contains _ }
  }
}
