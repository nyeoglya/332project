ThisBuild / version := "0.1.0"
ThisBuild / organization := "kr.ac.postech.green"
ThisBuild / organizationName := "green"
ThisBuild / scalaVersion := "2.13.15"

lazy val commonDependencies = Seq(
  "dev.zio" %% "zio" % "2.1.11",
  "dev.zio" %% "zio-streams" % "2.1.11"
)

lazy val workerDependencies = Seq(
  // Add dependencies here
)

lazy val masterDependencies = Seq(
  // Add dependencies here
)

lazy val worker = (project in file("worker"))
  .settings(
    name := "worker",
    libraryDependencies ++= commonDependencies ++ workerDependencies,
    assembly / mainClass := Some("Main"),
    assembly / assemblyJarName := s"${name.value}.jar",
  )

lazy val master = (project in file("master"))
  .settings(
    name := "master",
    libraryDependencies ++= commonDependencies ++ masterDependencies,
    assembly / mainClass := Some("Main"),
    assembly / assemblyJarName := s"${name.value}.jar",
  )

excludeFilter := HiddenFileFilter || "*.sc"
