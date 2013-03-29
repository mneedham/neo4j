package org.neo4j.cypher.internal.commands.expressions

import org.junit.Test
import org.neo4j.cypher.internal.pipes.QueryState
import org.neo4j.cypher.internal.ExecutionContext


class SubsetFunctionTest {
  @Test def canReturnARangeOfValuesInACollection() {
    val m = ExecutionContext.empty
    assert(SubsetFunction(Some(1), Some(2), Collection(Literal(1), Literal(2), Literal(3))).apply(m)(QueryState()) == Seq(1, 2))
    assert(SubsetFunction(Some(2), None, Collection(Literal(1), Literal(2), Literal(3))).apply(m)(QueryState()) == Seq(2,3))
    assert(SubsetFunction(None, Some(2), Collection(Literal(1), Literal(2), Literal(3))).apply(m)(QueryState()) == Seq(1,2))
    assert(SubsetFunction(None, None, Collection(Literal(1), Literal(2), Literal(3))).apply(m)(QueryState()) == Seq(1,2,3))
  }
}
