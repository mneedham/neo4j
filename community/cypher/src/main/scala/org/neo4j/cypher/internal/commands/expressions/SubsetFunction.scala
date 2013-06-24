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

import org.neo4j.cypher.internal.pipes.QueryState
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.helpers.CollectionSupport
import org.neo4j.cypher.internal.symbols.{AnyCollectionType, SymbolTable, CypherType}


case class SubsetFunction(begin: Option[Expression], end: Option[Expression], collection: Expression) extends NullInNullOutExpression(collection) with CollectionSupport with NumericHelper {
  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState) = {
    val coll = makeTraversable(value)
    (begin, end) match {
      case (Some(b), Some(e)) => {
        val (eInt, bInt):(Int,Int) = (asInt(e(m)), asInt(b(m)))
        if(eInt < 0 && bInt < 0)  coll.slice(coll.size+bInt,coll.size+eInt)
        else if(bInt < 0)      coll.slice(coll.size+bInt,eInt)
        else if (eInt < 0)     coll.slice(bInt,coll.size+eInt)
        else                coll.slice(bInt,eInt)
      }
      case (None, Some(e)) => if(asInt(e(m)) < 0) coll.take(coll.size+asInt(e(m))) else coll.take(asInt(e(m)))
      case (Some(b), None) => if(asInt(b(m)) < 0) coll.drop(coll.size+asInt(b(m))) else coll.drop(asInt(b(m)))
      case (None, None) => coll
    }
  }

  def rewrite(f: (Expression) => Expression) = f(SubsetFunction(
    begin match { 
      case Some(b) => Some(b.rewrite(f))
      case None => None
    },
    end match {
      case Some(e) => Some(e.rewrite(f))
      case None => None
    }, 
    collection.rewrite(f)))

  def children = Seq(collection)

  def identifierDependencies(expectedType: CypherType) = null

  def calculateType(symbols: SymbolTable) = collection.evaluateType(AnyCollectionType(), symbols).iteratedType

  def symbolTableDependencies = {
    val bDeps = begin match {
      case Some(b) => b.symbolTableDependencies
      case _ => List()
    }
    val eDeps = end match {
      case Some(e) => e.symbolTableDependencies
      case _ => List()
    }
    collection.symbolTableDependencies ++ bDeps ++ eDeps
  }
}
