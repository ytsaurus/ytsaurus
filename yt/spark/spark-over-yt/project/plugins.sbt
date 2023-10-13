resolvers += MavenCache("local-maven", Path.userHome / ".m2" / "repository")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.3")
libraryDependencies ++= Seq(
  "com.eed3si9n.jarjarabrams" %% "jarjar-abrams-core" % "1.9.0",
  "org.ow2.asm" % "asm" % "9.4",
  "org.ow2.asm" % "asm-commons" % "9.4"
)

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.4.1")

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")

libraryDependencies += "org.vafer" % "jdeb" % "1.3" artifacts (Artifact("jdeb", "jar", "jar"))
libraryDependencies += "com.github.eikek" %% "yamusca-core" % "0.5.1"

val circeVersion = "0.12.3"
libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

libraryDependencies ++= Seq(
  "tech.ytsaurus" % "ytsaurus-client" % "1.0.1" excludeAll (
    ExclusionRule(organization = "com.fasterxml.jackson.core")
  )
)

addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")

addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.2.1")

useCoursier := false
