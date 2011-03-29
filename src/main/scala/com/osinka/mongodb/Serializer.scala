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

import com.mongodb.DBObject

/**
 * Generic serializer of objects to/from {@link DBObject}
 *
 * @param T type <code>T</code> is meant to be a Scala object reprensenting
 * MongoDB's DBObject.
 * @see com.osinka.mongodb.MongoCollection
 */
trait Serializer[T] {
    /**
     * convert a domain object of type T to DBObject
     *
     * @param obj Scala object of type <code>T</code>
     * @return DBObject which holds object <code>obj</code>
     */
    def apply(obj: T): DBObject

    /**
     * convert a DBObject to domain object
     *
     * @param dbo DBObject
     * @return object read from <code>dbo</code>
     */
    def unapply(dbo: DBObject): Option[T]

    /**
     * Modify object to save DBObject identity. DBObjects store identity about the
     * document in the DB, this method mirrors this identity information onto the
     * domain object.
     *
     * @param x object which represents <code>dbo</code> in Scala world
     * @param dbo DBObject
     * @return object <code>x</code>
     */
    def mirror(x: T)(dbo: DBObject): T = x
}
