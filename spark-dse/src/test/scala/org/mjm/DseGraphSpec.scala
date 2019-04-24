package org.mjm

import com.datastax.bdp.graph.spark.graphframe._
import com.datastax.driver.dse.DseSession
import com.datastax.driver.dse.graph.{GraphResultSet, SimpleGraphStatement}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.functions.lit
import org.graphframes.{GraphFrame, examples}

class DseGraphSpec extends BaseSpec with LocalDseClusterTemplate {

  import spark.implicits._

  val graphName = "spark_dse_graph"

  "We" should "be able to access our Local DSE Node" in {
    cassandraConnector
      .withSessionDo(session => session.execute("SELECT * FROM system_schema.tables"))
      .all() should not be empty
  }

  it should "be able to do some work with Spark" in {
    spark.sparkContext.parallelize(1 to 10).count should be (10)
  }

  it should "be able to use Spark to load data into DSE Graph" in {

    def executeGraph(dseSession: DseSession)(graphName: String)(statement: String) : GraphResultSet = {
      dseSession.executeGraph(new SimpleGraphStatement(statement).setGraphName(graphName))
    }

    val connector = CassandraConnector(spark.sparkContext)
    val dseSession = connector.openSession().asInstanceOf[DseSession]

    val graphName = "spark_dse_graph"

    // load the example friends graph as a GraphFrame
    val g: GraphFrame = examples.Graphs.friends
    g.vertices.printSchema()
    g.edges.printSchema()

    g.vertices.show(false)
    g.edges.show(false)

    val graphExecutor = executeGraph(dseSession)(graphName) _

    // create and configure the graph
    dseSession.executeGraph(new SimpleGraphStatement(s"system.graph('${graphName}').ifNotExists().create()")
      .setSystemQuery())
    val schemaConfigs : List[String] = List("schema.config().option('graph.schema_mode').set('development')",
      "schema.config().option('graph.allow_scan').set('true')",
      "schema.clear()")
    graphExecutor(schemaConfigs.mkString("\n"))

    // create the schema
    val schema : List[String] = List("schema.propertyKey('age').Int().create()",
      "schema.propertyKey('name').Text().create()",
      "schema.propertyKey('id').Text().single().create()",
      "schema.vertexLabel('people').partitionKey('id').properties('name', 'age').create()",
      "schema.edgeLabel('friend').create()",
      "schema.edgeLabel('friend').connection('people', 'people').add()",
      "schema.edgeLabel('follow').create()",
      "schema.edgeLabel('follow').connection('people', 'people').add()")
    graphExecutor(schema.mkString("\n"))
    graphExecutor("schema.describe()")

    // import the GraphFrame to DSE Graph
    val d = spark.dseGraph(graphName)

    d.V.printSchema()
    d.E.printSchema()

    val v = g.vertices.select ($"id" as "_id", lit("people") as "~label", $"name", $"age")
    val e = g.edges.select (d.idColumn(lit("people"), $"src") as "src", d.idColumn(lit("people"), $"dst") as "dst",  $"relationship" as "~label")

    v.show(false)
    e.show(false)

    d.updateVertices(v)
    d.updateEdges(e)
  }

  // NOTE: dependent on the prior test loading data in the graph
  it should "be able to perform Graph Analytics with GraphFrames on data stores in DSE Graph" in {

    // import the GraphFrame to DSE Graph
    val dgf = spark.dseGraph(graphName)

    // convert DSE GraphFrame to Spark GraphFrame
    val g = dgf.gf

    // NOTE: The Following examples are taken from the Spark GraphFrames docs
    // http://graphframes.github.io/graphframes/docs/_site/user-guide.html

    // Run Label Propagation Algorithm for detecting communities in networks
    val result = g.labelPropagation.maxIter(5).run()
    result.select("id", "label", "name", "age").show(false)

    // Run PageRank until convergence to tolerance "tol".
    val results = g.pageRank.resetProbability(0.15).tol(0.01).run()
    // Display resulting pageranks and final edge weights
    results.vertices.select("id", "pagerank").show(false)
    results.edges.select("src", "dst", "weight").show(false)
  }

}
