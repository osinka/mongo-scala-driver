/**
 * Copyright (C) 2009 Osinka <http://osinka.ru>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.osinka.mongodb.shape

import com.osinka.mongodb._

/**
 * Query factory for ObjectShape
 * 
 * @author Alexander Azarov <azarov@osinka.com>
 */
trait Queriable[T] { self: ObjectShape[T] =>
    type SortableFieldType = ObjectField with ScalarContentConditions[_]

    /**
     * Empty query: any document qualifies
     */
    def any = ShapeQuery()

    /**
     * Query based on field conditions
     */
    def where(query: QueryTerm[T]): ShapeQuery = ShapeQuery() where query

    /**
     * Query that drops n first documents
     */
    def drop(n: Int) = ShapeQuery() drop n

    /**
     * Query that limits the resulting collection by n documents
     */
    def take(n: Int) = ShapeQuery() take n

    /**
     * Query where results are sorted
     */
    def sortBy(sorting: (SortableFieldType, SortOrder)*) = ShapeQuery().sortBy(sorting:_*)

    /**
     * Immutable query to apply to ShapedCollection
     */
    sealed case class ShapeQuery(val filters: QueryTerm[T], val sortBy: List[(SortableFieldType, SortOrder)], private val q: Query) {
        /**
         * Apply the query to collection
         */
        def in[Coll <: QueriedCollection[T, Coll]](coll: Coll): Coll = coll.applied(query)

        def where(filter: QueryTerm[T]): ShapeQuery = copy(filters = filters and filter)

        def drop(n: Int): ShapeQuery = drop(Some(n))
        def drop(n: Option[Int]): ShapeQuery = copy(q = q drop n)

        def take(n: Int): ShapeQuery = take(Some(n))
        def take(n: Option[Int]): ShapeQuery = copy(q = q take n)

        /**
         * modified query, no sorting
         */
        def noSort = ShapeQuery(filters, sortBy, q sort None)
        def sortBy(s: (SortableFieldType, SortOrder)*): ShapeQuery = copy(sortBy = s.toList ::: sortBy)

        def query: Query = {
            val s = (Map.empty[String, Int] /: sortBy) { (m, x) =>
                m + (x._1.longFieldName -> x._2.mongoOrder)
            }
            q ++ filters.dbo sort s
        }
    }

    /**
     * Factory of queries
     */
    object ShapeQuery {
        /**
         * Empty query
         */
        def apply() = new ShapeQuery(QueryTerm[T], Nil, Query())

        /**
         * Query based on field conditions
         */
        def apply(qt: QueryTerm[T]) = new ShapeQuery(qt, Nil, Query())
    }

    // TODO: Monadic query? http://github.com/alaz/mongo-scala-driver/issues#issue/13
}

/**
 * @author Alexander Azarov <azarov@osinka.com>
 */
sealed case class QueryTerm[+T](val qb: QueryBuilder) {
    def m = qb.m

    def dbo = qb.dbo

    def query = Query(dbo)

    def and[B >: T](q: QueryTerm[B]) = new QueryTerm[T](qb and q.qb)
}

/**
 * @author Alexander Azarov <azarov@osinka.com>
 */
object QueryTerm {
    def apply[T]() = new QueryTerm[T]( QueryBuilder() )
    def apply[T](tuple: (String, Any)) = new QueryTerm[T]( QueryBuilder(tuple) )
    def apply[T](m: Map[String,Any]) = new QueryTerm[T]( QueryBuilder(m) )
}
