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
       CREATE (neo4j:Database {name:'Neo4j'})
    """.stripMargin)

  override val setupConstraintQueries = List(
    "CREATE INDEX ON :Database(name)".stripMargin
  )



  def section = "Query Plan"

  @Test def allNodesScan() {
    profileQuery(
      title = "All Nodes Scan",
      text = "This query will return all nodes. It's not a good idea to run a query like this on a production database.",
      queryText = """MATCH (n) RETURN n""",
      optionalResultExplanation = """""",
      assertions = (p) => assertEquals(List(), List()))
  }

  @Test def nodeByLabelScan() {
    profileQuery(
      title = "Node by label scan",
      text = """This query will return all nodes which have label 'Person' where the property 'name' has the value 'me'
                via a scan of the Person labelindex""",
      queryText = """MATCH (person:Person {name: "me"}) return person""",
      optionalResultExplanation = """""",
      assertions = (p) => assertEquals(true, true))
  }

  @Test def nodeByIndexSeek() {
    profileQuery(
      title = "Node index seek",
      text = """This query will return all nodes which have label 'Company' where the property 'name' has the value
                'neo4j' using the Company index.""",
      queryText = """MATCH (neo4j:Database {name: "neo4j"}) return neo4j""",
      optionalResultExplanation = """""",
      assertions = (p) => assertEquals(true, true))
  }

}
