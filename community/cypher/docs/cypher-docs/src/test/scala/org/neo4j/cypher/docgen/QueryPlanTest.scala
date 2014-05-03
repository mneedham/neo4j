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
    """.stripMargin)

  override val setupConstraintQueries = List(
    "CREATE INDEX ON :Location(name)".stripMargin
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
      assertions = (p) => assertTrue(p.executionPlanDescription().toString.contains("Selection")))
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
}
