name := "myProject2"
version := "0.1"
scalaVersion := "2.10.5"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.2"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.2" exclude("org.glassfish.hk2", "hk2-utils") exclude("org.glassfish.hk2", "hk2-locator") exclude("javax.validation", "validation-api")
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.2" exclude("org.glassfish.hk2", "hk2-utils") exclude("org.glassfish.hk2", "hk2-locator") exclude("javax.validation", "validation-api")
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.2"  exclude("org.glassfish.hk2", "hk2-utils") exclude("org.glassfish.hk2", "hk2-locator") exclude("javax.validation", "validation-api")

// https://mvnrepository.com/artifact/com.databricks/spark-csv
libraryDependencies += "com.databricks" %% "spark-csv" % "1.4.0"
