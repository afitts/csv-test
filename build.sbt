name := "csv-test"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.storm"          %  "storm-core"         % "1.2.2"            % "compile",
  "org.apache.storm"          %  "storm-kafka-client" % "1.2.2"            % "compile",
  "org.scalatest"             %% "scalatest"          % "2.2.6"            % "test",
  "com.github.tototoshi"      %% "scala-csv"          % "1.3.6",
  "org.apache.httpcomponents" %  "httpclient"         % "4.5.9"            % "compile",
  "org.apache.kafka"          %% "kafka"              % "2.2.0" % Compile,
  "org.apache.kafka"          %  "kafka-clients"      % "2.2.0" % Compile
)

//libraryDependencies += "org.apache.storm" % "storm-core" % "1.2.2" % "compile"

//libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

//libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.6"

//libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.9" % "compile"

scalacOptions ++= Seq("-feature", "-deprecation", "-Yresolve-term-conflict:package")

// When doing sbt run, fork a separate process.  This is apparently needed by storm.
fork := true

resolvers += "clojars" at "https://clojars.org/repo"

assemblyMergeStrategy in assembly := {
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", xs @ _*) =>
    xs map {_.toLowerCase} match {
      case "manifest.mf" :: Nil | "index.list" :: Nil | "dependencies" :: Nil =>
        MergeStrategy.discard
      case ps @ x :: `xs` if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: `xs` =>
        MergeStrategy.discard
      case "services" :: `xs` =>
        MergeStrategy.filterDistinctLines
      case "spring.schemas" :: Nil | "spring.handlers" :: Nil =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first}
