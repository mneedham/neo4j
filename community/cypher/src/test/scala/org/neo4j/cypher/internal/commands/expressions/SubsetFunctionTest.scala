package org.neo4j.cypher.internal.commands.expressions

import org.junit.Test
import org.neo4j.cypher.internal.pipes.QueryState
import org.neo4j.cypher.internal.ExecutionContext
import org.junit.Assert.assertEquals;


class SubsetFunctionTest {
  @Test def canReturnARangeOfValuesInACollection() {
    val m = ExecutionContext.empty
    val collection = Collection(Literal(0), Literal(1), Literal(2), Literal(3))

    assertEquals(Seq(), SubsetFunction(Some(0), Some(0), collection).apply(m)(QueryState()))
    assertEquals(Seq(0), SubsetFunction(Some(0), Some(1), collection).apply(m)(QueryState()))
    assertEquals(Seq(), SubsetFunction(Some(1), Some(1), collection).apply(m)(QueryState()))
    assertEquals(Seq(1,2,3), SubsetFunction(Some(1), None, collection).apply(m)(QueryState()))
    assertEquals(Seq(0,1,2), SubsetFunction(Some(0), Some(-1), collection).apply(m)(QueryState()))
    assertEquals(Seq(1,2), SubsetFunction(Some(1), Some(-1), collection).apply(m)(QueryState()))
    assertEquals(Seq(0,1,2), SubsetFunction(None, Some(-1), collection).apply(m)(QueryState()))
    assertEquals(Seq(1,2), SubsetFunction(Some(-3), Some(-1), collection).apply(m)(QueryState()))
    assertEquals(Seq(2,3), SubsetFunction(Some(-2), None, collection).apply(m)(QueryState()))
  }
}
