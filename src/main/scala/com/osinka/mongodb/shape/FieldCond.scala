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

import java.util.regex.Pattern
import scala.util.matching.Regex
import com.osinka.mongodb._
import wrapper.{MongoCondition,DBO}
import MongoCondition._

/**
 * Order direction base trait
 * 
 * @author Alexander Azarov <azarov@osinka.com>
 */
sealed trait SortOrder {
    def mongoOrder: Int
}

/**
 * Methods to build constraints on fields.
 * 
 * @author Alexander Azarov <azarov@osinka.com>
 */
trait FieldQueryConditions[T, QueryType] { shape: ShapeFields[T, QueryType] =>

    /**
     * Basic all-purpose field conditions. Applicable to a field of any kind
     */
    trait FieldConditions[A] { self: MongoField[A] =>
        protected def mkCond(f: (String,Any) => (String,Any), x: Option[Any]) =
            x map {v => QueryTerm[QueryType](f(longFieldName, v)) } getOrElse QueryTerm[QueryType]()

        def exists = QueryTerm[QueryType](MongoCondition.exists(longFieldName, true))

        def notExists = QueryTerm[QueryType](MongoCondition.exists(longFieldName, false))
    }


    /**
     * Field conditions applicable to scalar fields
     */
    trait ScalarContentConditions[A] extends FieldConditions[A] { self: MongoField[A] with ScalarContent[A] =>
        // Conditions
        def is_<(x: A) = mkCond(lt, serialize(x))

        def is_<=(x: A) = mkCond(le, serialize(x))

        def is_>(x: A) = mkCond(gt, serialize(x))

        def is_>=(x: A) = mkCond(ge, serialize(x))

        def is(x: A) = mkCond(eqTest, serialize(x))
        def is_==(x: A) = is(x)
        def eq_?(x: A) = is(x)
        def has(x: A) = is(x) // same for occurence in array

        def isNot(x: A) = mkCond(neTest, serialize(x))
        def not_==(x: A) = isNot(x)
        def ne_?(x: A) = not_==(x)

        def isIn(x: List[A]) = mkCond(MongoCondition.in, Some(x flatMap { serialize }) )
        def in(x: List[A]) = isIn(x)

        def notIn(x: List[A]) = mkCond(MongoCondition.nin, Some(x flatMap { serialize }) )
        def nin(x: List[A]) = notIn(x)

        def hasAll(x: List[A]) = mkCond(MongoCondition.all, Some(x flatMap { serialize }) )
        def all(x: List[A]) = hasAll(x)

        def hasSize(x: Int) = mkCond(size, Some(x))
        def ofSize(x: Int) = hasSize(x)

        def like(x: Pattern) = QueryTerm[QueryType](regex(longFieldName, x))
        def is_~(x: Pattern) = like(x)

        def like(x: Regex) = QueryTerm[QueryType](regex(longFieldName, x))
        def is_~(x: Regex) = like(x)

        // Sorting
        case object Asc  extends SortOrder { override val mongoOrder = 1}
        case object Desc extends SortOrder { override val mongoOrder = -1 }

        /**
         * ascending order by this field
         */
        def ascending  = this -> Asc

        /**
         * descending order by this field
         */
        def descending = this -> Desc
    }

    /**
     * Field conditions applicable to reference fields
     */
    trait RefContentConditions[V <: MongoObject] extends FieldConditions[V] { self: MongoField[V] with RefContent[V] =>
        // Conditions
        def is(x: V) = x.mongoOID map { oid =>
                val fieldId = longFieldName+"._id"
                QueryTerm[QueryType]( eqTest(fieldId, oid) )
            }  getOrElse notExists
        def is_==(x: V) = is(x)
        def eq_?(x: V) = is(x)
        def has(x: V) = is(x) // same for occurence in array

        def isNot(x: V) = x.mongoOID map { oid =>
                val fieldId = longFieldName+"._id"
                QueryTerm[QueryType]( neTest(fieldId, oid) )
            } getOrElse exists
        def not_==(x: V) = isNot(x)
        def ne_?(x: V) = not_==(x)

        def isIn(x: List[V]) = {
            val fieldId = longFieldName+"._id"
            QueryTerm[QueryType]( MongoCondition.in(fieldId, x flatMap {_.mongoOID}) )
        }
        def in(x: List[V]) = isIn(x)

        def notIn(x: List[V]) = {
            val fieldId = longFieldName+"._id"
            QueryTerm[QueryType]( MongoCondition.nin(fieldId, x flatMap {_.mongoOID}) )
        }
        def nin(x: List[V]) = notIn(x)
    }
}