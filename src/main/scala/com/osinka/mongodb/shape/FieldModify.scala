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
import wrapper.MongoOp

/**
 * Update operations on fields
 */
trait FieldModifyOperations[T, QueryType] { shape: ShapeFields[T, QueryType] =>

    /**
     * Basic field update operations trait.
     */
    trait BaseFieldModifyOp { self: ObjectField =>
        protected def mkOp(f: (String,Any) => (String,Any), x: Option[Any]) =
            x map {v => ModifyOp[QueryType](f(longFieldName, v)) } getOrElse ModifyOp[QueryType]()

        def unset: ModifyOp[QueryType] = mkOp(MongoOp.unset, Some(1))
    }

    /**
     * Field update operations, applicable to fields of any kind
     */
    trait FieldModifyOp[A] extends BaseFieldModifyOp { self: MongoField[A] with FieldContent[A] =>
        def set(x: A): ModifyOp[QueryType] = mkOp(MongoOp.set, serialize(x))
    }

    /**
     * Field update operations, applicable to scalar fields
     */
    trait ScalarFieldModifyOp[A] extends FieldModifyOp[A] { self: MongoScalar[A] with ScalarContent[A] =>
        def inc(x: A): ModifyOp[QueryType] = mkOp(MongoOp.inc, serialize(x))
    }

    /**
     * Field update operations, applicable to array fields
     */
    trait ArrayFieldModifyOp[A] extends BaseFieldModifyOp { self: MongoArray[A] with FieldContent[A] =>
        def set(x: Seq[A]): ModifyOp[QueryType] = mkOp(MongoOp.set, Some(x flatMap { serialize }) )
        def push(x: A): ModifyOp[QueryType] = mkOp(MongoOp.push, serialize(x))
        def pushAll(x: Iterable[A]): ModifyOp[QueryType] = mkOp(MongoOp.pushAll, Some(x flatMap { serialize }) )
        def popHead: ModifyOp[QueryType] = mkOp(MongoOp.pop, Some(-1))
        def popTail: ModifyOp[QueryType] = mkOp(MongoOp.pop, Some(1) )
        def pull(x: A): ModifyOp[QueryType] = mkOp(MongoOp.pull, serialize(x))
        def pullAll(x: Iterable[A]): ModifyOp[QueryType] = mkOp(MongoOp.pullAll, Some(x flatMap { serialize }) )
        def addToSet(x: A): ModifyOp[QueryType] = mkOp(MongoOp.addToSet, serialize(x) )
        def addToSet(x: Iterable[A]): ModifyOp[QueryType] = mkOp(MongoOp.addEachToSet, Some(x flatMap { serialize }) )
     }
}

// TODO: Monadic query? http://github.com/alaz/mongo-scala-driver/issues#issue/13
// TODO: unified ModifyOp with QueryTerm??
sealed case class ModifyOp[+T](val qb: QueryBuilder) {
    def dbo = qb.dbo
    def query = Query(dbo)
    def and[B >: T](q: ModifyOp[B]) = new ModifyOp[T](qb and q.qb)
}

object ModifyOp {
    def apply[T]() = new ModifyOp[T]( QueryBuilder() )
    def apply[T](tuple: (String, Any)) = new ModifyOp[T]( QueryBuilder(tuple) )
}
