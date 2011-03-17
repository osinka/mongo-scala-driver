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

import scala.reflect.Manifest
import com.mongodb.{DBObject, DBCollection}
import com.osinka.mongodb._
import wrapper.DBO

/**
 * Shape of an object held in some other object (being it a Shape or Query). This trait
 * is most generic and used to declare embedded fields mostly.
 */
trait ObjectIn[T, QueryType] extends Serializer[T] with ShapeFields[T, QueryType] {
    /**
     * Every Shape must provide the list of the fields in the documents of this shape.
     */
    def * : List[MongoField[_]]

    /**
     * Every Shape must provide the factory to create object T
     * @param dbo the document in MongoDB
     * @return None if it's impossible to retrieve object T from dbo
     */
    def factory(dbo: Option[DBObject]): Option[T]

    protected def fieldList: List[MongoField[_]] = *

    /**
     * Document constraint
     */
    lazy val constraints = fieldList filterNot {_.mongoInternal_?} map {_.mongoConstraints} reduceLeft {_ and _}

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

    // -- Serializer[T]
    override def in(x: T): DBObject = packFields(x, fieldList)

    override def out(dbo: DBObject) = factory(Some(dbo)) map { x =>
        updateFields(x, dbo, fieldList)
        x
    }

    override def mirror(x: T)(dbo: DBObject) = {
        updateFields(x, dbo, fieldList filter { _.mongoInternal_? })
        x
    }
}

/**
 * Shape of an object backed by DBObject ("hosted in")
 */
trait ObjectShape[T] extends ObjectIn[T, T] with Queriable[T] {
    /**
     * Make a collection of T elements
     * @param underlying MongoDB collection
     * @return ShapedCollection based on this ObjectShape
     */
    def collection(underlying: DBCollection) = new ShapedCollection[T](underlying, this)
}

/**
 * Mix-in to make a shape functional
 *
 * FunctionalShape makes a shape with convinient syntactic sugar
 * for converting object to DBObject (apply) and extractor for the opposite
 *
 * E.g.
 * val dbo = UserShape(u)
 * dbo match {
 *    case UserShape(u) =>
 * }
 */
trait FunctionalShape[T] { self: ObjectShape[T] =>
    def apply(x: T): DBObject = in(x)
    def unapply(rep: DBObject): Option[T] = out(rep)
}

/**
 * Shape of MongoObject child.
 *
 * It has mandatory _id and _ns fields
 */
trait MongoObjectShape[T <: MongoObject] extends ObjectShape[T] {
    import org.bson.types.ObjectId

    /**
     * MongoDB internal Object ID field declaration
     */
    lazy val oid = Field.optional("_id", (x: T) => x.mongoOID, (x: T, oid: Option[ObjectId]) => x.mongoOID = oid)

    // -- ObjectShape[T]
    override def fieldList : List[MongoField[_]] = oid :: super.fieldList
}