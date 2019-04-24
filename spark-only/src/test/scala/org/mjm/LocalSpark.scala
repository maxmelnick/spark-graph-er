package org.mjm

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait LocalSpark extends BeforeAndAfterAll { self: Suite =>
  val spark = SparkSession.builder
    .master("local[2]")
    .appName("Test Application")
    .config("spark.sql.warehouse.dir", "./spark-warehouse")
    .getOrCreate()

  override def beforeAll() {
    super.beforeAll()
    val checkpointDir = Files.createTempDirectory(this.getClass.getName).toString
    spark.sparkContext.setCheckpointDir(checkpointDir)
  }

  override def afterAll() {
    val checkpointDir = spark.sparkContext.getCheckpointDir
//    if (spark != null) {
//      spark.stop()
//    }
    checkpointDir.foreach { dir =>
      FileUtils.deleteQuietly(new File(dir))
    }
    FileUtils.deleteQuietly(new File("./spark-warehouse"))
    super.afterAll()
  }
}
