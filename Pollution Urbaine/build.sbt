ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "Pollution_Urbaine",
    libraryDependencies ++= Seq(
      // Spark Core & SQL
      "org.apache.spark" %% "spark-core" % "4.0.1",
      "org.apache.spark" %% "spark-sql"  % "4.0.1",

      // Spark MLlib (for Pipeline, VectorAssembler, Regression, etc.)
      "org.apache.spark" %% "spark-mllib" % "4.0.1",
      "org.apache.spark" %% "spark-mllib-local" % "4.0.1", // optional for local ML

      // Spark GraphX (for Step 4)
      "org.apache.spark" %% "spark-graphx" % "4.0.1"
    )
  )
