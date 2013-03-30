/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands.expressions

import org.junit.Test
import org.neo4j.cypher.internal.pipes.QueryState
import org.neo4j.cypher.internal.ExecutionContext
import org.junit.Assert.assertEquals;


class SubsetFunctionTest {
  @Test def canReturnARangeOfValuesInACollection() {
    val m = ExecutionContext.empty
    val collection = Collection(Literal(0), Literal(1), Literal(2), Literal(3))

    assertEquals(Seq(), SubsetFunction(Some(Literal(0)), Some(Literal(0)), collection).apply(m)(QueryState()))
    assertEquals(Seq(0), SubsetFunction(Some(Literal(0)), Some(Literal(1)), collection).apply(m)(QueryState()))
    assertEquals(Seq(), SubsetFunction(Some(Literal(1)), Some(Literal(1)), collection).apply(m)(QueryState()))
    assertEquals(Seq(1,2,3), SubsetFunction(Some(Literal(1)), None, collection).apply(m)(QueryState()))
    assertEquals(Seq(0,1,2), SubsetFunction(Some(Literal(0)), Some(Literal(-1)), collection).apply(m)(QueryState()))
    assertEquals(Seq(1,2), SubsetFunction(Some(Literal(1)), Some(Literal(-1)), collection).apply(m)(QueryState()))
    assertEquals(Seq(0,1,2), SubsetFunction(None, Some(Literal(-1)), collection).apply(m)(QueryState()))
    assertEquals(Seq(1,2), SubsetFunction(Some(Literal(-3)), Some(Literal(-1)), collection).apply(m)(QueryState()))
    assertEquals(Seq(2,3), SubsetFunction(Some(Literal(-2)), None, collection).apply(m)(QueryState()))
  }
}
