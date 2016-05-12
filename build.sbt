organization := "data-graft"

name := "Sparker"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.10.6"

// set the main Scala source directory to be <base>/src
scalaSource in Compile := baseDirectory.value / "src"

// set the Scala test source directory to be <base>/test
scalaSource in Test := baseDirectory.value / "test"

// set the main class for packaging the main jar
// 'run' will still auto-detect and prompt
// change Compile to Test to set it for the test jar
mainClass in(Compile, packageBin) := Some("net.datagraft.sparker.ScalableGrafter")

// set the main class for the main 'run' task
// change Compile to Test to set it for 'test:run'
mainClass in(Compile, run) := Some("net.datagraft.sparker.ScalableGrafter")

// set the prompt (for this build) to include the project id.
shellPrompt in ThisBuild := { state => Project.extract(state).currentRef.project + "> " }

// set the prompt (for the current project) to include the username
shellPrompt := { state => System.getProperty("user.name") + "> " }

// disable printing timing information, but still print [success]
showTiming := true

// disable printing a message indicating the success or failure of running a task
showSuccess := true

// fork a new JVM for 'run' and 'test:run'
fork := true

// fork a new JVM for 'test:run', but not 'run'
fork in Test := true

// add a JVM option to use when forking a JVM for 'run'
javaOptions += "-Xmx1G"

// only use a single thread for building
parallelExecution := false


// change the format used for printing task completion time
timingFormat := {
  import java.text.DateFormat
  DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT)
}

publishTo := Some(Resolver.file("file", new File(Path.userHome.absolutePath + "/.m2/repository")))

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "com.databricks" %% "spark-csv" % "1.4.0",
  "org.apache.spark" %% "spark-graphx" % "1.6.0"
  //      "log4j" % "log4j" % "1.2.15" excludeAll(
  //        ExclusionRule(organization = "com.sun.jdmk"),
  //        ExclusionRule(organization = "com.sun.jmx"),
  //        ExclusionRule(organization = "javax.jms")
  //        )
)

