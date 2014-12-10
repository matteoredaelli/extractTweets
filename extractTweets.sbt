name := "Extract Tweets"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

unmanagedBase := baseDirectory.value / "custom_lib"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.1"

