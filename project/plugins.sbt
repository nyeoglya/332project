addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")

libraryDependencies +=
  "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % "0.6.3"
libraryDependencies +=
  "com.thesamet.scalapb" %% "compilerplugin" % "0.11.11"
