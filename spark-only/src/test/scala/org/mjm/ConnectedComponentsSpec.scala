package org.mjm

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.collect_list
import org.graphframes.GraphFrame

class ConnectedComponentsSpec extends BaseTestSpec with LocalSpark {

  behavior of "ConnectedComponentsExample"

  it should "simplify entity canonicalization" in {
    import spark.implicits._

    // Create the initial graph structure
    val v = Seq(1, 3, 4, 5, 6, 7).toDF("id")
    val e = Seq((1, 4), (3, 6), (1, 5), (4, 7), (5, 7), (7, 1)).toDF("src", "dst")
    val g = GraphFrame(v, e)

    val components: DataFrame = g.connectedComponents.run()
    components.show()
    //    +---+---------+
    //    | id|component|
    //    +---+---------+
    //    |  1|        1|
    //    |  3|        3|
    //    |  4|        1|
    //    |  5|        1|
    //    |  6|        3|
    //    |  7|        1|
    //    +---+---------+

    val resolvedEntities = components.groupBy($"component" as "resolved_entity_id")
      .agg(collect_list($"id") as "raw_entity_ids")
    resolvedEntities.show(false)
    //    +------------------+--------------+
    //    |resolved_entity_id|raw_entity_ids|
    //    +------------------+--------------+
    //    |1                 |[1, 4, 5, 7]  |
    //    |3                 |[3, 6]        |
    //    +------------------+--------------+

  }

}
