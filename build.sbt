name := "hnstream"

version := "1.0"

scalaVersion := "2.11.6"

//libraryDependencies += "org.msgpack" %% "msgpack-scala" % "0.6.11"
//libraryDependencies += "org.msgpack" % "msgpack-core" % "0.7.0-p7"

libraryDependencies += "com.julianpeeters" % "avro-scala-macro-annotations_2.11" % "0.5"
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0-M5" cross CrossVersion.full)
