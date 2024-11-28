ThisBuild / version := "0.1.0"
ThisBuild / organization := "kr.ac.postech.green"
ThisBuild / organizationName := "green"
ThisBuild / scalaVersion := "2.13.15"

val grpcVersion = "1.64.0"
val zioVersion = "2.1.12"

Compile / PB.targets := Seq(
  scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb",
  scalapb.zio_grpc.ZioCodeGenerator -> (Compile / sourceManaged).value / "scalapb"
)

lazy val commonDependencies = Seq(
  "io.grpc" % "grpc-netty" % grpcVersion,
  "io.grpc" % "grpc-protobuf" % grpcVersion,
  "io.grpc" % "grpc-stub" % grpcVersion,
  "dev.zio" %% "zio" % zioVersion,
  "dev.zio" %% "zio-streams" % zioVersion,
  "junit" % "junit" % "4.10" % Test,
  "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.rogach" %% "scallop" % "5.1.0",
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
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
  .settings(
  )

lazy val common = (project in file("common"))
  .settings(
    name := "common",
    libraryDependencies ++= commonDependencies,
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb",
      scalapb.zio_grpc.ZioCodeGenerator -> (Compile / sourceManaged).value / "scalapb"
    )
  )
  .disablePlugins(AssemblyPlugin)

lazy val worker = (project in file("worker"))
  .settings(
    name := "worker",
    libraryDependencies ++= commonDependencies ++ workerDependencies,
    assembly / mainClass := Some("Main"),
    assembly / assemblyJarName := s"${name.value}.jar",
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb",
      scalapb.zio_grpc.ZioCodeGenerator -> (Compile / sourceManaged).value / "scalapb"
    )
  )
  .dependsOn(common)

lazy val master = (project in file("master"))
  .settings(
    name := "master",
    libraryDependencies ++= commonDependencies ++ masterDependencies,
    assembly / mainClass := Some("Main"),
    assembly / assemblyJarName := s"${name.value}.jar",
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb",
      scalapb.zio_grpc.ZioCodeGenerator -> (Compile / sourceManaged).value / "scalapb"
    )
  )
  .dependsOn(common)

excludeFilter := HiddenFileFilter || "*.sc"
