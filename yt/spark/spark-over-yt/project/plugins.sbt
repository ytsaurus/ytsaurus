resolvers += MavenCache("local-maven", Path.userHome / ".m2" / "repository")

addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.9.16")

addDependencyTreePlugin

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")

libraryDependencies += "org.vafer" % "jdeb" % "1.3" artifacts (Artifact("jdeb", "jar", "jar"))

libraryDependencies ++= Seq(
  "tech.ytsaurus" % "ytsaurus-client" % "1.2.0" excludeAll (
    ExclusionRule(organization = "com.fasterxml.jackson.core")
  )
)

addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")

addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.2.1")

addSbtPlugin("com.github.sbt" % "sbt-javaagent" % "0.1.8")

useCoursier := false
