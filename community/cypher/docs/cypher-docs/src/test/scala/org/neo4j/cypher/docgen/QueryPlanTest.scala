/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.docgen

import org.junit.Test
import org.junit.Assert._
import org.neo4j.graphdb.Node
import org.neo4j.visualization.graphviz.GraphStyle
import org.neo4j.visualization.graphviz.AsciiDocSimpleStyle

class QueryPlanTest extends DocumentingTestBase {
  override val setupQueries = List(
    """CREATE (me:Person {name:'me'})
       CREATE (andres:Person {name:'Andres'})
       CREATE (andreas:Person {name:'Andreas'})
       CREATE (malmo:Location {name:'Malmo'})
       CREATE (london:Location {name:'London'})
       CREATE (field:Team {name:'Field'})
       CREATE (engineering:Team {name:'Engineering'})
       CREATE (me)-[:WORKS_IN]->(london)
       CREATE (me)-[:FRIENDS_WITH]->(andres)
       CREATE (andres)-[:FRIENDS_WITH]->(andreas)
    """.stripMargin)

  override val setupConstraintQueries = List(
    "CREATE INDEX ON :Location(name)".stripMargin,
    "CREATE CONSTRAINT ON (team:Team) ASSERT team.name is UNIQUE".stripMargin
  )

  def section = "Query Plan"

  @Test def allNodesScan() {
    profileQuery(
      title = "All Nodes Scan",
      text = "This query will return all nodes. It's not a good idea to run a query like this on a production database.",
      queryText = """MATCH (n) RETURN n""",
      optionalResultExplanation = """""",
      assertions = (p) => assertTrue(p.executionPlanDescription().toString.contains("AllNodesScan")))
  }

  @Test def nodeByLabelScan() {
    profileQuery(
      title = "Node by label scan",
      text = """This query will return all nodes which have label 'Person' where the property 'name' has the value 'me'
                via a scan of the Person label index""",
      queryText = """MATCH (person:Person {name: "me"}) RETURN person""",
      optionalResultExplanation = """""",
      assertions = (p) => assertTrue(p.executionPlanDescription().toString.contains("LabelScan")))
  }

  @Test def nodeByIndexSeek() {
    profileQuery(
      title = "Node index seek",
      text = """This query will return all nodes which have label 'Company' where the property 'name' has the value
                'Malmo' using the Location index.""",
      queryText = """MATCH (location:Location {name: "Malmo"}) RETURN location""",
      optionalResultExplanation = """""",
      assertions = (p) => assertTrue(p.executionPlanDescription().toString.contains("NodeIndexSeek")))
  }

  @Test def nodeByUniqueIndexSeek() {
    profileQuery(
      title = "Node unique index seek",
      text = """This query will return all nodes which have label 'Team' where the property 'name' has the value
                'Field' using the Team unique index.""",
      queryText = """MATCH (team:Team {name: "Field"}) RETURN team""",
      optionalResultExplanation = """""",
      assertions = (p) => assertTrue(p.executionPlanDescription().toString.contains("NodeUniqueIndexSeek")))
  }

  @Test def nodeByIdSeek() {
    profileQuery(
      title = "Node by Id seek",
      text = """This query will return the node which has nodeId 0""",
      queryText = """MATCH n WHERE id(n) = 0 RETURN n""",
      optionalResultExplanation = """""",
      assertions = (p) => assertTrue(p.executionPlanDescription().toString.contains("NodeByIdSeek")))
  }

  @Test def projection() {
    profileQuery(
      title = "Projection",
      text = """This query will produce one row with the value 'hello'.""",
      queryText = """RETURN "hello" AS greeting""",
      optionalResultExplanation = """""",
      assertions = (p) => assertTrue(p.executionPlanDescription().toString.contains("Projection")))
  }

  @Test def selection() {
    profileQuery(
      title = "Selection",
      text =
        """This query will look for nodes with the label 'Person' and filter those whose name
           begins with the letter 'a'.""",
      queryText = """MATCH (p:Person) WHERE p.name =~ "^a.*" RETURN p""",
      optionalResultExplanation = """""",
      assertions = (p) => assertTrue(p.executionPlanDescription().toString.contains("Filter")))
//      assertions = (p) => assertTrue(p.executionPlanDescription().toString.contains("Selection")))
  }

  @Test def cartesianProduct() {
    profileQuery(
      title = "Cartesian Product",
      text =
        """This query will join all the people with all the locations and return the cartesian product of the nodes
          with those labels.
        """.stripMargin,
      queryText = """MATCH (p:Person), (l:Location) RETURN p, l""",
      optionalResultExplanation = """""",
      assertions = (p) => assertTrue(p.executionPlanDescription().toString.contains("CartesianProduct")))
  }

  @Test def optionalMatch() {
    profileQuery(
      title = "Optional Match",
      text =
        """This query will find all the people and the location they work in if there is one.
        """.stripMargin,
      queryText = """MATCH (p:Person) OPTIONAL MATCH (p)-[:WORKS_IN]->(l) RETURN p, l""",
      optionalResultExplanation = """""",
      assertions = (p) =>  {
        println(p.executionPlanDescription().toString)
        assertTrue(p.executionPlanDescription().toString.contains("Optional"))
      })
  }

  @Test def sort() {
    profileQuery(
      title = "Sort",
      text =
        """This query will find all the people and return them sorted alphabetically by name.
        """.stripMargin,
      queryText = """MATCH (p:Person) RETURN p ORDER BY p.name""",
      optionalResultExplanation = """""",
      assertions = (p) =>  {
        println(p.executionPlanDescription().toString)
        assertTrue(p.executionPlanDescription().toString.contains("Sort"))
      })
  }

  @Test def sortedLimit() {
    profileQuery(
      title = "Sorted Limit",
      text =
        """This query will find the first 2 people sorted alphabetically by name.
        """.stripMargin,
      queryText = """MATCH (p:Person) RETURN p ORDER BY p.name LIMIT 2""",
      optionalResultExplanation = """""",
      assertions = (p) =>  {
        println(p.executionPlanDescription().toString)
        assertTrue(p.executionPlanDescription().toString.contains("SortedLimit"))
      })
  }

  @Test def limit() {
    profileQuery(
      title = "Limit",
      text =
        """This query will return the first 3 people in an arbitrary order.
        """.stripMargin,
      queryText = """MATCH (p:Person) RETURN p LIMIT 3""",
      optionalResultExplanation = """""",
      assertions = (p) =>  {
        println(p.executionPlanDescription().toString)
        assertTrue(p.executionPlanDescription().toString.contains("Limit"))
      })
  }

  @Test def expand() {
    profileQuery(
      title = "Expand",
      text =
        """This query will return my friends of friends.
        """.stripMargin,
      queryText = """MATCH (p:Person {name: "me"})-[:FRIENDS_WITH*2]->(fof) RETURN fof""",
      optionalResultExplanation = """""",
      assertions = (p) =>  {
        println(p.executionPlanDescription().toString)
        assertTrue(p.executionPlanDescription().toString.contains("Expand"))
      })
  }

  @Test def selectOrSemiApply() {
    profileQuery(
      title = "Select Or Semi Apply",
      text =
        """This query will find all the people who aren't my friend.
        """.stripMargin,
      queryText =
        """MATCH (p:Person {name: "me"}), (other:Person)
           WHERE NOT((me)-[:FRIENDS_WITH]->(other))
           RETURN other""",
      optionalResultExplanation = """""",
      assertions = (p) =>  {
        println(p.executionPlanDescription().toString)
        assertTrue(p.executionPlanDescription().toString.contains("SelectOrSemiApply"))
      })
  }
}
