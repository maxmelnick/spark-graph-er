package org.mjm

import org.apache.spark.sql.DataFrame
import org.graphframes.GraphFrame

case class Person(personId: Int, name: String, address: String, phone: String, email: String)

class FlexibleCandidateSelectionSpec extends BaseTestSpec with LocalSpark {

  import spark.implicits._

  behavior of "FlexibleCandidateSelection"

  private val customersDf = Seq(
    Person(1, "Jane Doe", "123 Main St.", "555-123-1600", "jd@emailz.com"),
    Person(2, "Jane Smith", "123 Main St.", "555-123-1600", ""),
    Person(3, "Jane Smith", "", "", "jd@emailz.com"),
    Person(4, "John Smith", "123 Main St.", "555-321-1600", "js@emailz.com")
  ).toDF()

  customersDf.write.saveAsTable("customers")

  // NOTE: we use the same Spark GraphFrame motif-finding query for each graph example
  private def getCandidatesGraph(g: GraphFrame): DataFrame = {
    g.find("(a)-[e]->(b); (c)-[e2]->(b)")
      .filter($"a.type" === "person")
      .filter($"c.type" === "person")
      .filter($"a.id" =!= $"c.id")
      .filter($"a.id" < $"c.id")
      .select($"a.id" as "personId1", $"c.id" as "personId2")
      .dropDuplicates()
  }

  it should "demonstrate candidate selection by phone via SparkSQL and GraphFrames" in {

    // SparkSQL Example ////////////////////////////////////////////////////////////////////////////////////////////////

    val sqlStatement =
       """
         | select c1.personId as personId1, c2.personId as personId2
         | from customers c1 join customers c2 on c1.phone = c2.phone
         | WHERE c1.personId < c2.personId
       """.stripMargin

    val sqlCandidatePairs = spark.sql(sqlStatement)

    sqlCandidatePairs.show()
    //    +---------+---------+
    //    |personId1|personId2|
    //    +---------+---------+
    //    |        1|        2|
    //    +---------+---------+



    // Spark GraphFrames Example ///////////////////////////////////////////////////////////////////////////////////////

    val v = Seq(
      (1, "Jane Doe", "person"), (2, "Jane Smith", "person"),
      (3, "Jane Smith", "person"), (4, "John Smith", "person"),
      (5, "555-123-1600", "phone"), (8, "555-321-1600", "phone")
    ).toDF("id", "label", "type")

    val e = Seq(
      (1, 5, "has_phone"), (2, 5, "has_phone"), (4, 8, "has_phone")
    ).toDF("src", "dst", "relationship")

    val g = GraphFrame(v, e)

    val graphCandidatePairs = getCandidatesGraph(g)

    graphCandidatePairs.show(false)
    //    +---------+---------+
    //    |personId1|personId2|
    //    +---------+---------+
    //    |1        |2        |
    //    +---------+---------+

    sqlCandidatePairs.collect().shouldEqual(graphCandidatePairs.collect())

  }

  it should "demonstrate candidate selection by phone or address via SparkSQL and GraphFrames" in {

    // SparkSQL Example ////////////////////////////////////////////////////////////////////////////////////////////////

    val sqlStatement =
      """
        | select c1.personId as personId1, c2.personId as personId2
        | from customers c1 join customers c2 on c1.phone = c2.phone
        | WHERE c1.personId < c2.personId
        | UNION
        | select c1.personId as personId1, c2.personId as personId2
        | from customers c1 join customers c2 on c1.address = c2.address
        | WHERE c1.personId < c2.personId
      """.stripMargin

    val sqlCandidatePairs = spark.sql(sqlStatement)

    sqlCandidatePairs.show()
    //    +---------+---------+
    //    |personId1|personId2|
    //    +---------+---------+
    //    |        1|        2|
    //    |        1|        4|
    //    |        2|        4|
    //    +---------+---------+



    // Spark GraphFrames Example ///////////////////////////////////////////////////////////////////////////////////////

    // We add address vertices and edges to the graph structure, but the candidate selection query remains the same

    val v = Seq(
      (1, "Jane Doe", "person"), (2, "Jane Smith", "person"),
      (3, "Jane Smith", "person"), (4, "John Smith", "person"),
      (5, "555-123-1600", "phone"), (7, "123 Main St.", "address"),
      (8, "555-321-1600", "phone")
    ).toDF("id", "label", "type")

    val e = Seq(
      (1, 5, "has_phone"), (2, 5, "has_phone"), (1, 7, "lives_at"),
      (2, 7, "lives_at"), (4, 8, "has_phone"), (4, 7, "lives_at")
    ).toDF("src", "dst", "relationship")

    val g = GraphFrame(v, e)

    val graphCandidatePairs = getCandidatesGraph(g)

    graphCandidatePairs.show(false)
    //    +---------+---------+
    //    |personId1|personId2|
    //    +---------+---------+
    //    |1        |2        |
    //    |1        |4        |
    //    |2        |4        |
    //    +---------+---------+

    sqlCandidatePairs.collect().shouldEqual(graphCandidatePairs.collect())

  }

  it should "demonstrate candidate selection by phone or address or email via SparkSQL and GraphFrames" in {

    // SparkSQL Example ////////////////////////////////////////////////////////////////////////////////////////////////

    val sqlStatement =
       """
         | select c1.personId as personId1, c2.personId as personId2
         | from customers c1 join customers c2 on c1.phone = c2.phone
         | WHERE c1.personId < c2.personId
         | UNION
         | select c1.personId as personId1, c2.personId as personId2
         | from customers c1 join customers c2 on c1.address = c2.address
         | WHERE c1.personId < c2.personId
         | UNION
         | select c1.personId as personId1, c2.personId as personId2
         | from customers c1 join customers c2 on c1.email = c2.email
         | WHERE c1.personId < c2.personId
       """.stripMargin

    val sqlCandidatePairs = spark.sql(sqlStatement)

    sqlCandidatePairs.show()
    //    +---------+---------+
    //    |personId1|personId2|
    //    +---------+---------+
    //    |        1|        2|
    //    |        1|        3|
    //    |        1|        4|
    //    |        2|        4|
    //    +---------+---------+



    // Spark GraphFrames Example ///////////////////////////////////////////////////////////////////////////////////////

    // We add email vertices and edges to the graph structure, but the candidate selection query remains the same

    val v = Seq((1, "Jane Doe", "person"), (2, "Jane Smith", "person"),
      (3, "Jane Smith", "person"), (4, "John Smith", "person"),
      (5, "555-123-1600", "phone"), (6, "jd@emailz.com", "email"),
      (7, "123 Main St.", "address"), (8, "555-321-1600", "phone"),
      (9, "js@emailz.com", "email")
    ).toDF("id", "label", "type")

    val e = Seq((1, 5, "has_phone"), (2, 5, "has_phone"), (1, 7, "lives_at"),
      (2, 7, "lives_at"), (1, 6, "has_email"), (3, 6, "has_email"),
      (4, 8, "has_phone"), (4, 9, "has_email"), (4, 7, "lives_at")
    ).toDF("src", "dst", "relationship")

    val g = GraphFrame(v, e)

    val graphCandidatePairs = getCandidatesGraph(g)

    graphCandidatePairs.show(false)
    //    +---------+---------+
    //    |personId1|personId2|
    //    +---------+---------+
    //    |1        |2        |
    //    |1        |3        |
    //    |1        |4        |
    //    |2        |4        |
    //    +---------+---------+

    sqlCandidatePairs.collect().shouldEqual(graphCandidatePairs.collect())

  }

}
