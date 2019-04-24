package org.mjm

import java.io.File
import java.nio.file.Files

import com.datastax.driver.dse.DseSession
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait LocalDseClusterTemplate extends BeforeAndAfterAll { self: Suite =>
  @transient var sqlContext: SQLContext = _

  private def sparkConf: SparkConf = {
    val _ipAddress = "127.0.0.1"
    val byosParameters = Seq(
      ("spark.hadoop.cassandra.host", _ipAddress),
      ("spark.hadoop.cassandra.auth.kerberos.enabled", "false"),
      ("spark.cassandra.auth.conf.factory", "com.datastax.bdp.spark.DseByosAuthConfFactory"),
      ("spark.hadoop.fs.dsefs.impl", "com.datastax.bdp.fs.hadoop.DseFileSystem"),
      ("spark.hadoop.dse.advanced_replication.directory", "/var/lib/cassandra/advrep"),
      ("spark.hadoop.com.datastax.bdp.fs.client.authentication.factory", "com.datastax.bdp.fs.hadoop.DseRestClientAuthProviderBuilderFactory"),
      ("spark.cassandra.connection.port", "9042"), ("spark.hadoop.cassandra.ssl.enabled", "false"),
      ("spark.hadoop.cassandra.auth.kerberos.defaultScheme", "false"),
      ("spark.hadoop.cassandra.client.transport.factory", "com.datastax.bdp.transport.client.TDseClientTransportFactory"),
      ("spark.cassandra.connection.host", _ipAddress),
      ("spark.hadoop.fs.cfs.impl", "com.datastax.bdp.hadoop.cfs.CassandraFileSystem"),
      ("spark.hadoop.cassandra.connection.native.port", "9042"),
      ("spark.hadoop.dse.client.configuration.impl", "com.datastax.bdp.transport.client.HadoopBasedClientConfiguration"),
      ("spark.cassandra.connection.factory", "com.datastax.bdp.spark.DseCassandraConnectionFactory"),
      ("spark.hadoop.cassandra.config.loader", "com.datastax.bdp.config.DseConfigurationLoader"),
      ("spark.hadoop.cassandra.connection.rpc.port", "9160"),
      ("spark.sql.hive.metastore.sharedPrefixes", "com.typesafe.scalalogging"),
      ("spark.hadoop.dse.system_memory_in_mb", "63996"),
      ("spark.hadoop.cassandra.thrift.framedTransportSize", "15728640"),
      ("spark.hadoop.cassandra.partitioner", "org.apache.cassandra.dht.Murmur3Partitioner"),
      ("spark.hadoop.cassandra.dsefs.port", "5598"))

    val conf = new SparkConf()
    byosParameters.foreach(p => conf.set(p._1, p._2))
    conf
  }


  lazy val spark: SparkSession = {
    SparkSession
      .builder
      .appName("Test")
      .config("spark.master","local[2]")
      .config("spark.sql.shuffle.partitions", "4")
      .config(sparkConf)
      .getOrCreate
  }

  @transient val cassandraConnector: CassandraConnector = {
    CassandraConnector(sparkConf)
  }

  @transient val dseSession: DseSession = {
    cassandraConnector.openSession().asInstanceOf[DseSession]
  }

  override def beforeAll() {
    super.beforeAll()
    val checkpointDir = Files.createTempDirectory(this.getClass.getName).toString
    spark.sparkContext.setCheckpointDir(checkpointDir)
  }

  override def afterAll() {
    val checkpointDir = spark.sparkContext.getCheckpointDir
    sqlContext = null
    if (spark != null) {
      spark.stop()
    }
    checkpointDir.foreach { dir =>
      FileUtils.deleteQuietly(new File(dir))
    }
    super.afterAll()
  }
}

