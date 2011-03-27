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

package com.osinka.mongodb.shape

import com.mongodb.{DBCollection, DBObject}
import com.osinka.mongodb._
import wrapper._

/**
 * Collecton of type T elements, where serializer is an ObjectShape
 */
class ShapedCollection[T](override val underlying: DBCollection, val shape: ObjectShape[T])
        extends MongoCollection[T]
        with QueriedCollection[T, ShapedCollection[T]] {

    /**
     * Update elements
     * @param multi should update all elements
     */
    def update(filters: QueryTerm[T], op: ModifyOp[T], multi: Boolean): Boolean = update(filters.dbo, op.dbo, multi)

    /**
     * Update only one element
     */
    def updateOne(filters: QueryTerm[T], op: ModifyOp[T]): Boolean = update(filters, op, false)

    /**
     * Update all matching elements
     */
    def update(filters: QueryTerm[T], op: ModifyOp[T]): Boolean = update(filters, op, true)

    /**
     * Remove many elements
     */
    def -=(filters: QueryTerm[T]) {
      remove(filters.dbo)
    }

    /**
     * Find and remove the first found document
     */
    def findAndRemove(filters: QueryTerm[T]): Option[T] = findAndRemove(filters.dbo)

    def findAndModify(q: ObjectShape[T]#ShapeQuery, op: ModifyOp[T]): Option[T] =
      findAndModify(q, op, false, false, false)

    /**
     * Find and modify the first found document (or create it (or modify after create))
     */
    def findAndModify(q: ObjectShape[T]#ShapeQuery, op: ModifyOp[T], remove: Boolean, returnNew: Boolean, upsert: Boolean): Option[T] = {
      val query = q.query
      findAndModify(query.query, query.sorting, op.dbo, remove, returnNew, upsert)
    }

    def findAndModify(qt: QueryTerm[T], op: ModifyOp[T]): Option[T] =
      findAndModify(qt, op, false, false, false)

    def findAndModify(qt: QueryTerm[T], op: ModifyOp[T], remove: Boolean, returnNew: Boolean, upsert: Boolean): Option[T] =
      findAndModify(shape.ShapeQuery(qt), op, remove, returnNew, upsert)

    // -- MongoCollection[T]
    override val serializer: Serializer[T] = shape

    // -- QueriedCollection[T]
    override val query: Query = Query.empty
    override def applied(q: Query): ShapedCollection[T] = new ShapedCollection[T](underlying, shape) {
        override val query = q
    }

    // -- MongoCollection
    override def stringPrefix: String = "ShapedCollection["+shape.getClass.getName+"]("+getName+")"
}
