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

import com.mongodb.DBCollection
import com.osinka.mongodb._

/**
 * @author Alexander Azarov <azarov@osinka.com>
 */
trait Implicits {
    implicit def collOfShape(coll: DBCollection) = new {
        def of[T](element: ObjectShape[T]) = element.collection(coll)
    }

    implicit def collWithQuery[T](qt: QueryTerm[T]) = new {
        def in(coll: ShapedCollection[T]): ShapedCollection[T] = coll.shape where qt in coll
    }

    implicit def queryTofilters[T](q: Queriable[T]#ShapeQuery): QueryTerm[T] = q.filters
}