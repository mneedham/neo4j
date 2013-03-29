package org.neo4j.cypher.internal.commands.expressions

import org.neo4j.cypher.internal.pipes.QueryState
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.helpers.CollectionSupport
import org.neo4j.cypher.internal.symbols.{AnyCollectionType, SymbolTable, CypherType}


case class SubsetFunction(begin: Option[Int], end: Option[Int], collection: Expression) extends NullInNullOutExpression(collection) with CollectionSupport {
  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState) = {
    val coll = makeTraversable(value)
    if (coll.size == 0)
      null
    else
      (begin, end) match {
        case (Some(b), Some(e)) => coll.take(e).drop(b-1)
        case (None, Some(e)) => coll.take(e)
        case (Some(b), None) => coll.drop(b-1)
        case (None, None) => coll
      }
  }

  def rewrite(f: (Expression) => Expression) = f(SubsetFunction(begin, end, collection.rewrite(f)))

  def children = Seq(collection)

  def identifierDependencies(expectedType: CypherType) = null

  def calculateType(symbols: SymbolTable) = collection.evaluateType(AnyCollectionType(), symbols).iteratedType

  def symbolTableDependencies = collection.symbolTableDependencies
}
