ThisBuild / version := "0.1.0"
ThisBuild / organization := "kr.ac.postech.green"
ThisBuild / organizationName := "green"
ThisBuild / scalaVersion := "2.13.15"

PB.targets in Compile := Seq(
  scalapb.gen(grpc = true) -> (sourceManaged in Compile).value / "scalapb",
  scalapb.zio_grpc.ZioCodeGenerator -> (sourceManaged in Compile).value / "scalapb"
)

lazy val commonDependencies = Seq(
  "dev.zio" %% "zio" % "2.1.11",
  "dev.zio" %% "zio-streams" % "2.1.11",
  "junit" % "junit" % "4.10" % Test,
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.rogach" %% "scallop" % "5.1.0",
  "io.grpc" % "grpc-netty" % "1.39.0",
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion
)

lazy val workerDependencies = Seq(
  // Add dependencies here
)

lazy val masterDependencies = Seq(
  // Add dependencies here
)

lazy val global = project
  .in(file("."))
  .disablePlugins(AssemblyPlugin)
  .aggregate(
    common,
    worker,
    master
  )

lazy val common = (project in file("common"))
  .settings(
    name := "common",
    libraryDependencies ++= commonDependencies,
  )
  .disablePlugins(AssemblyPlugin)

lazy val worker = (project in file("worker"))
  .settings(
    name := "worker",
    libraryDependencies ++= commonDependencies ++ workerDependencies,
    assembly / mainClass := Some("Main"),
    assembly / assemblyJarName := s"${name.value}.jar",
  )
  .dependsOn(common)

lazy val master = (project in file("master"))
  .settings(
    name := "master",
    libraryDependencies ++= commonDependencies ++ masterDependencies,
    assembly / mainClass := Some("Main"),
    assembly / assemblyJarName := s"${name.value}.jar",
  )
  .dependsOn(common)

excludeFilter := HiddenFileFilter || "*.sc"
