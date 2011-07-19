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
package com.osinka.mongodb

import com.mongodb.{DBCollection, DBObject}

/**
 * Collection of DBObjects
 *
 * @author Alexander Azarov <azarov@osinka.ru>
 * @see PlainDBOSerializer
 */
class DBObjectCollection(override val underlying: DBCollection)
        extends MongoCollection[DBObject]
        with QueriedCollection[DBObject, DBObjectCollection] {

    override val serializer: Serializer[DBObject] = PlainDBOSerializer

    // -- QueriedCollection[T]
    override val query: Query = Query.empty
    override def applied(q: Query): DBObjectCollection = new DBObjectCollection(underlying) {
        override val query = q
    }

    // -- MongoCollection
    override def stringPrefix: String = "DBObjectCollection("+getName+")"

    override def <<(o: DBObject) = underlying.insert(o)

    override def <<(objs: Seq[DBObject]) = underlying.insert(objs.toArray, underlying.getWriteConcern)

    override def <<?(obj: DBObject): Option[DBObject] = {
        underlying insert obj
        underlying.getDB.getLastError get "err" match {
            case null => Some(obj)
            case msg: String => None
        }
    }

    override def +=(obj: DBObject) { underlying.save(obj) }

    override def -=(obj: DBObject) { underlying.remove(obj) }
}

/**
 * Serializer of DBObject to DBObject: does nothing and passes the same
 * DBObject through
 * 
 * @author Alexander Azarov <azarov@osinka.ru>
 */
object PlainDBOSerializer extends Serializer[DBObject] {
    override def apply(obj: DBObject) = obj
    override def unapply(dbo: DBObject) = Some(dbo)
    override def mirror(x: DBObject)(dbo: DBObject) = dbo
}
