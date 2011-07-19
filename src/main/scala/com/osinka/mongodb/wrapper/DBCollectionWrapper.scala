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
package com.osinka.mongodb.wrapper

import com.mongodb.{DBCollection, DBObject}

/**
 * Wrapper for MongoDB DBCollection
 * 
 * @author Alexander Azarov <azarov@osinka.com>
 */
trait DBCollectionWrapper {
    /**
     * Actual DBCollection object behind
     */
    val underlying: DBCollection

    protected def find(dbo: DBObject) = underlying find dbo
    protected def findOne(dbo: DBObject) = underlying findOne dbo
    protected def getCount(dbo: DBObject) = underlying getCount dbo

    /**
     * MongoDB collection name
     */
    def getName = underlying.getName

    /**
     * MongoDB full collection name
     */
    def getFullName = underlying.getFullName

    /**
     * Remove collection
     */
    def drop: Unit = underlying.drop

    override def equals(obj: Any) = obj match {
        case other: DBCollectionWrapper => underlying.equals(other.underlying)
        case _ => false
    }
}