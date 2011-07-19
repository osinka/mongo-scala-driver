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

import com.mongodb.{DBObject, DBCollection}
import com.osinka.mongodb._
import wrapper.DBO

/**
 * Shape of an object held in some other object (being it a Shape or Query). This trait
 * is most generic and used to declare embedded fields mostly.
 * 
 * @author Alexander Azarov <azarov@osinka.ru>
 */
trait ObjectIn[T, QueryType] extends ShapeFields[T, QueryType] {
    type FactoryPF = PartialFunction[DBObject, T]

    /**
     * Every Shape must provide the factory to create object T or subtype.
     * PartialFunction[DBObject,T]
     */
    def factory: FactoryPF

    /**
     * Override this if you want to control which fields are stored per-object.
     */
    def objectFields(x: T): List[MongoField[_]] = allFields

    private[shape] def packFields(x: T, fields: Seq[MongoField[_]]): DBObject =
        DBO.fromMap( (fields foldLeft Map[String,Any]() ) { (m,f) =>
            f.mongoReadFrom(x) match {
                case Some(v) => m + (f.mongoFieldName -> v)
                case None => m
            }
        } )

    private[shape] def updateFields(x: T, dbo: DBObject, fields: Seq[MongoField[_]]) {
        fields foreach { f => f.mongoWriteTo(x, Option(dbo get f.mongoFieldName)) }
    }

    def pack(x: T): DBObject = packFields(x, objectFields(x))

    def unpack(dbo: DBObject) = factory.lift(dbo) map { x =>
        updateFields(x, dbo, objectFields(x))
        x
    }
}

/**
 * Shape of an object backed by DBObject ("hosted in")
 * 
 * @author Alexander Azarov <azarov@osinka.ru>
 */
trait ObjectShape[T] extends Serializer[T] with ObjectIn[T, T] with Queriable[T] {
    // -- Serializer[T]
    def apply(x: T): DBObject = pack(x)

    def unapply(dbo: DBObject): Option[T] = unpack(dbo)

    override def mirror(x: T)(dbo: DBObject) = {
        updateFields(x, dbo, objectFields(x))
        x
    }

    /**
     * Make a collection of T elements
     * @param underlying MongoDB collection
     * @return ShapedCollection based on this ObjectShape
     */
    def collection(underlying: DBCollection) = new ShapedCollection[T](underlying, this)
}

/**
 * Shape of MongoObject child.
 *
 * It has mandatory _id and _ns fields
 * 
 * @author Alexander Azarov <azarov@osinka.ru>
 */
trait MongoObjectShape[T <: MongoObject] extends ObjectShape[T] {
    import org.bson.types.ObjectId

    /**
     * MongoDB internal Object ID field declaration
     */
    lazy val oid = Field.optional("_id", (x: T) => x.mongoOID, (x: T, oid: Option[ObjectId]) => x.mongoOID = oid)
}
