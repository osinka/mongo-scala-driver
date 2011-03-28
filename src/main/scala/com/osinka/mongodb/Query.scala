/**
 * Copyright (C) 2009 Alexander Azarov <azarov@osinka.com>
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

package com.osinka.mongodb

import org.bson.types.ObjectId
import com.mongodb.{DBObject, BasicDBObject}
import wrapper._

/**
 * Immutable MongoDB query object.
 */
case class Query(final val query: DBObject,
                 val skip: Option[Int],
                 val limit: Option[Int],
                 val sorting: Option[DBObject]) {

    def slice_? = skip.isDefined || limit.isDefined || sorting.isDefined

    /**
     * @return the query where n first elements are dropped.
     * @param n sets the skip parameter if Some; removes the parameter if None
     */
    def drop(n: Option[Int]) = copy(skip = n)

    /**
     * @return the query limited by first n elements
     * @param n sets the limit parameter if Some; removes the parameter if None
     */
    def take(n: Option[Int]) = copy(limit = n)

    /**
     * @return the query where n first elements are dropped.
     */
    def drop(n: Int): Query = drop(Some(n))

    /**
     * @return the query limited by first n elements
     */
    def take(n: Int): Query = take(Some(n))

    /**
     * @return the query with sorting
     * @param s sets the sorting if Some; removes the sorting if None
     */
    def sort(s: Option[DBObject]): Query = copy(sorting = s)

    /**
     * @return the query with sorting
     */
    def sort(s: DBObject): Query = sort(Some(s))

    /**
     * Merge two queries. The filter part (query) is merged, while skip, limit and
     * sorting are taken from q
     */
    def *(q: Query): Query = ++(q.query) drop q.skip take q.limit sort q.sorting

    /**
     * Add filtering
     */
    def ++(q: DBObject): Query = Query(DBO.merge(query, q), skip, limit, sorting)
}

/**
 * Query factory
 */
object Query {
    /**
     * Empty query
     */
    final val empty: Query = Query(DBO.empty, None, None, None)

    /**
     * @return empty query
     */
    def apply(): Query = empty

    /**
     * @return the query with filters defined by q
     */
    def apply(q: DBObject) = new Query(q, None, None, None)

    /**
     * @return the query to find the object by its ID
     */
    def byId(oid: ObjectId) = apply(DBO.fromMap(Map("_id" -> oid)))
}

sealed case class QueryBuilder(val m: Map[String, Any]) {
    def dbo = DBO.fromMap(m)
    def and(q: QueryBuilder) = {
        def mergeMaps(ms: Map[String,Any]*)(f: (Any, Any) => Any) =
            (Map[String,Any]() /: ms.flatMap{x => x}) { (m, kv) =>
                m + (if (m contains kv._1) kv._1 -> f(m(kv._1), kv._2)
                     else kv)
            }

        def coincidence(v1: Any, v2: Any): Any = (v1, v2) match {
            case (m1: Map[_,_], m2: Map[_,_]) =>
                mergeMaps(m1.asInstanceOf[Map[String,Any]], m2.asInstanceOf[Map[String,Any]]) {coincidence}
//            case (m1: Map[String,Any], m2: Map[String,Any]) =>
//                mergeMaps(m1, m2) {coincidence}
            case _ => v2
        }

        new QueryBuilder( mergeMaps(m, q.m) {coincidence} )
    }
}

object QueryBuilder {
    def apply() = new QueryBuilder(Map.empty[String, Any])
    def apply(tuple: (String, Any)) = new QueryBuilder(Map(tuple))
}


/**
 * Mix-in for MongoCollection descendants. Modifies the behavior so that the query is
 * applied
 */
trait QueriedCollection[T, This <: QueriedCollection[T, This]] extends MongoCollection[T] {
    /**
     * @return the query to apply
     */
    def query: Query

    /**
     * @return new collection with the query q
     */
    def applied(q: Query): This

    // -- MongoCollection[T]
    override def find = find(query)
    override def headOption = find(query).toIterable.headOption
    override def sizeEstimate = getCount(query)
}
