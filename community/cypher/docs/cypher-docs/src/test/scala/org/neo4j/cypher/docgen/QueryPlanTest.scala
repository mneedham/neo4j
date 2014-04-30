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
    "CREATE (me {name:'me'})")

  def section = "Query Plan"

  @Test def returnNode() {
    profileQuery(
      title = "Profile something",
      text = "To return a node, list it in the `RETURN` statement.",
      queryText = """match (n {name: "me"}) return n.name""",
      optionalResultExplanation = """The example will return the node.""",
      assertions = (p) => assertEquals(List(Map("n.name" -> "me")), p.toList))
  }

}
