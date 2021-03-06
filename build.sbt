name := "jobbroker-queue"
organization := "com.ruimo"
scalaVersion := "2.12.6"

publishTo := Some(
  Resolver.file(
    "jobbroker-common",
    new File(Option(System.getenv("RELEASE_DIR")).getOrElse("/tmp"))
  )
)

resolvers += "ruimo.com" at "http://static.ruimo.com/release"

libraryDependencies += "com.ruimo" %% "jobbroker-common" % "1.0"
libraryDependencies += "com.rabbitmq" % "amqp-client" % "5.7.3"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25"

libraryDependencies += "ch.qos.logback" % "logback-core" % "1.2.3" % Test
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3" % Test
libraryDependencies += "com.github.fridujo" % "rabbitmq-mock" % "1.0.3" % Test
libraryDependencies += "com.ruimo" %% "scoins" % "1.17" % Test
libraryDependencies += "org.specs2" %% "specs2-core" % "4.3.2" % Test

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
