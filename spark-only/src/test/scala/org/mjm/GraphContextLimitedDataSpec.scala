package org.mjm

import org.graphframes.GraphFrame

class GraphContextLimitedDataSpec extends BaseTestSpec with LocalSpark {

  import spark.implicits._

  behavior of "GraphContextLimitedData"

  it should "allow using graph context for candidate selection when data is limited" in {

    val v = Seq((1, "Jane"), (2, "John"), (3, "John")).toDF("id", "name")
    val e = Seq((1, 2, "poc"), (1,3, "poc")).toDF("src", "dst", "relationship")

    val g = GraphFrame(v, e)

    val candidatePairs = g.find("(a)-[e]->(b); (a)-[e2]->(c)")
      .filter($"e.relationship" === $"e2.relationship")
      .filter($"b.id" =!= $"c.id")
      .filter($"b.id" < $"c.id")
      .select($"b", $"c")

    candidatePairs.show()

    //    +---------+---------+
    //    |        b|        c|
    //    +---------+---------+
    //    |[2, John]|[3, John]|
    //    +---------+---------+


  }
}
