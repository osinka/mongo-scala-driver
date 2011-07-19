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

import org.bson.types.ObjectId
import com.mongodb.DBObject
import com.osinka.mongodb._
import wrapper.DBO

/**
 * Field declaration in a Shape
 * 
 * @author Alexander Azarov <azarov@osinka.com>
 */
trait ObjectField {
    /**
     * Field name. It will be the key in MongoDB document
     */
    def mongoFieldName: String

    private[shape] def mongoFieldPath: List[String] = List(mongoFieldName)

    /**
     * Long field names separated by dot are required for queries and modificators
     */
    lazy val longFieldName = DBO.dotNotation(mongoFieldPath)
    
    override def hashCode = longFieldName.hashCode

    override def equals(other: Any): Boolean = other match {
        case that: ObjectField => (that canEqual this) && this.longFieldName == that.longFieldName
        case _ => false
    }

    def canEqual(other: Any): Boolean = true
}

/**
 * Abstract field container. Sometimes documents are nested one inside another.
 * 
 * @author Alexander Azarov <azarov@osinka.com>
 */
trait FieldContainer {
    private[shape] def containerPath: List[String] = Nil
}

/**
 * Field declaration builders for a Shape
 * 
 * @author Alexander Azarov <azarov@osinka.com>
 */
trait ShapeFields[T, QueryType] extends FieldContainer
        with FieldQueryConditions[T, QueryType] with FieldModifyOperations[T, QueryType] { parent =>

    /**
     * MongoDB field
     *
     * @see ObjectShape
     */
    trait MongoField[A] extends ObjectField with FieldConditions[A] with BaseFieldModifyOp { storage: FieldContent[A] =>
        /**
         * @return true if the field is internal MongoDB's field
         */
        def mongoInternal_? : Boolean = mongoFieldName startsWith "_"

        /**
         * @return constraint for the field to be part of Shape (currently it's the key existance)
         */
        def mongoConstraints: QueryTerm[QueryType] = rep postprocess storage.contentConstraints

        private[shape] def mongoReadFrom(x: T): Option[Any]
        private[shape] def mongoWriteTo(x: T, v: Option[Any])

        /**
         * @return Scala field representation object
         */
        def rep: FieldRep[_]

        def kindString: String
        override def toString: String =
            getClass.getName+"{"+kindString+"/"+storage.contentString+">"+rep.toString+"}("+longFieldName+")"

        // -- ObjectField
        override def mongoFieldPath: List[String] = parent.containerPath ::: super.mongoFieldPath
    }

    /**
     * Scalar MongoDB field
     */
    trait MongoScalar[A] extends MongoField[A] { storage: FieldContent[A] =>
        /**
         * useful method for field, like
         *   dbo match { case field(value) => ... }
         *
         * or in case of mandatory constructor argument
         *   for {field(v) <- Some(dbo)} yield new Obj(...., v, ...)
         */
        def unapply(dbo: DBObject): Option[A] = Option(dbo get mongoFieldName) flatMap storage.deserialize

        /**
         * in case of optional field
         *   new Obj(..., field from dbo, ...)
         */
        def from(dbo: DBObject) = unapply(dbo)

        // -- MongoField[A]
        override def rep: FieldRep[A]

        private[shape] def mongoReadFrom(x: T): Option[Any] =
            rep.get(x) flatMap storage.serialize

        private[shape] def mongoWriteTo(x: T, v: Option[Any]) {
            rep.put(x)(v flatMap storage.deserialize)
        }

        override def kindString = "Scalar"
    }

    /**
     * Array MongoDB field
     */
    trait MongoArray[A] extends MongoField[A] { storage: FieldContent[A] =>
        protected def unpackField(dbo: DBObject) = DBO.toArray(dbo).flatMap{Option[Any](_)}.flatMap{storage.deserialize}

        /**
         * useful method for field, like
         *   dbo match { case field(value) => ... }
         *
         * or in case of mandatory constructor argument
         *   for {field(v) <- Some(dbo)} yield new Obj(...., v, ...)
         */
        def unapply(dbo: DBObject): Option[Seq[A]] = Option(dbo get mongoFieldName) map { case dbo: DBObject => unpackField(dbo) }

        /**
         * in case of optional field
         *   new Obj(..., field from dbo, ...)
         */
        def from(dbo: DBObject) = unapply(dbo)

        // -- MongoField[A]
        override def rep: FieldRep[Seq[A]]

        // An array constraints only array field existance and nothing more.
        // It could set a constraint on the array's values, but array can be empty
        override def mongoConstraints = exists

        private[shape] def mongoReadFrom(x: T): Option[Any] =
            rep.get(x) map { _ map storage.serialize }

        private[shape] def mongoWriteTo(x: T, v: Option[Any]) {
            rep.put(x)(v map { case dbo: DBObject => unpackField(dbo) })
        }

        override def kindString = "Array"
    }

    trait MongoMap[A] extends MongoField[A] { storage: FieldContent[A] =>
        // TODO: condition on maps value: it depends on Content type.

        protected def unpackField(dbo: DBObject) = {
            def jclMap(dbo: DBObject) = {
                import scala.collection.mutable.{Map => MMap}
                import scala.collection.JavaConversions._
                
                val m: MMap[String,Any] = dbo.toMap.asInstanceOf[java.util.Map[String,Any]]
                m
            }

            (jclMap(dbo) foldLeft Map[String,A]() ) {(m,e) =>
                storage.deserialize(e._2) match {
                    case Some(v) => m + (e._1 -> v)
                    case None => m
                }
            }
        }

        /**
         * useful method for field, like
         *   dbo match { case field(value) => ... }
         *
         * or in case of mandatory constructor argument
         *   for {field(v) <- Some(dbo)} yield new Obj(...., v, ...)
         */
        def unapply(dbo: DBObject): Option[Map[String,A]] = Option(dbo get mongoFieldName) map {
            case dbo: DBObject => unpackField(dbo)
        }

        /**
         * in case of optional field
         *   new Obj(..., field from dbo, ...)
         */
        def from(dbo: DBObject) = unapply(dbo)

        // -- MongoField[A]
        override def rep: FieldRep[Map[String,A]]

        // Constraints only the field existance (we do not know keys)
        override def mongoConstraints = exists

        private[shape] def mongoReadFrom(x: T): Option[Any] = {
            def serializeValues(f: Map[String,A]) = (f foldLeft Map[String,Any]() ) { (m,e) =>
                storage.serialize(e._2) match {
                    case Some(v) => m + (e._1 -> v)
                    case None => m
                }
            }

            rep.get(x) map { m => DBO.fromMap( serializeValues(m) ) }
        }

        private[shape] def mongoWriteTo(x: T, v: Option[Any]) {
            rep.put(x)(v map { case dbo: DBObject => unpackField(dbo) })
        }

        override def kindString = "Map"
    }

    /**
     * Field content: scalar, ref, embedded
     */
    trait FieldContent[A] { self: ObjectField =>
        /**
         * serializes a field into DBObject value
         * @return None if cannot serialize
         */
        protected def serialize(x: A): Option[Any]

        /**
         * Reads field value from a DBObject value
         * @return None if cannot deserialize
         */
        protected def deserialize(v: Any): Option[A]
        
        /**
         * Constraints on the content. MongoField will call it, see the
         * description there.
         * @see MongoField
         */
        protected def contentConstraints: QueryTerm[QueryType]

        def contentString: String
    }

    /**
     * Scalar field content
     */
    trait ScalarContent[A] extends FieldContent[A] with ScalarContentConditions[A] { self: MongoField[A] =>
        // -- FieldContent[A]
        override def contentConstraints = exists

        override def serialize(a: A): Option[Any] = Some(a)
        override def deserialize(v: Any): Option[A] = Some(v.asInstanceOf[A])

        override def contentString = "Scalar"
    }

    /**
     * Reference field content
     */
    trait RefContent[V <: MongoObject] extends FieldContent[V] with RefContentConditions[V] { self: MongoField[V] =>
        protected val coll: MongoCollection[V]

        // -- FieldContent[A]
        override def serialize(a: V): Option[Any] = a.mongoOID map {oid =>
            DBO.fromMap(Map(
                "_ref" -> coll.getName,
                "_id" -> oid
            ))
        }
        override def deserialize(v: Any): Option[V] = v match {
            case dbo: DBObject if dbo.containsField("_id") =>
                dbo.get("_id") match {
                    case oid: ObjectId if coll.isDefinedAt(oid) => Some(coll(oid))
                    case _ => None
                }
            case _ => None
        }
        override def contentConstraints = exists

        override def contentString = "Ref"
    }

    /**
     * Embedded (nested document) field content
     */
    trait EmbeddedContent[V] extends FieldContent[V] with FieldContainer { objectShape: MongoField[V] with ObjectIn[V, QueryType] =>
        // -- FieldContainer
        override def containerPath = mongoFieldPath

        // -- FieldContent[A]
        override def contentConstraints = objectShape.constraints

        override def serialize(a: V): Option[Any] = Some(objectShape.in(a))
        override def deserialize(v: Any): Option[V] = v match {
            case dbo: DBObject => objectShape.out(dbo)
            case _ => None
        }

        override def contentString = "Embedded"
    }

    /**
     * Representation of a field in Scala objects
     */
    trait FieldRep[A] {
        /**
         * Post-process the content's field constraint as/if needed
         */
        def postprocess(constraints: QueryTerm[QueryType]) = constraints

        /**
         * Getter: get a field value from the object
         */
        def get[A1>:A](x: T): Option[A1]

        /**
         * Setter: set a field in the object
         */
        def put[A2<:A](x: T)(a: Option[A2])
    }

    /**
     * typical representation implementations
     */
    object Represented {
        /**
         * FieldRep implemented as field getter and setter
         */
        def by[A](g: T => A, p: Option[(T, A) => Unit]) = new FieldRep[A] {
            override def get[A1>:A](x: T): Option[A1] = Some(g(x))
            override def put[A2<:A](x: T)(a: Option[A2]) {
                for {func <- p; value <- a} func(x, value)
            }

            override def toString = "field"
        }

        /**
         * FieldRep implemented as Option[A] field getter and setter
         */
        def byOption[A](g: T => Option[A], p: Option[(T, Option[A]) => Unit]) = new FieldRep[A] {
            override def postprocess(constraints: QueryTerm[QueryType]) = QueryTerm[QueryType]()
            override def get[A1>:A](x: T): Option[A1] = g(x)
            override def put[A2<:A](x: T)(a: Option[A2]) {
                for {func <- p} func(x, a)
            }

            override def toString = "Option"
        }
    }

    /**
     * Scalar field
     * @param mongoFieldName document key
     * @param g field getter
     * @param p optional field setter
     */
    class ScalarField[A](override val mongoFieldName: String, val g: T => A, val p: Option[(T,A) => Unit])
            extends MongoScalar[A] with ScalarContent[A] with ScalarFieldModifyOp[A] {
        override val rep = Represented.by(g, p)
        override def canEqual(other: Any): Boolean = other.isInstanceOf[ScalarField[_]]
    }

    /*
     * Optional field
     * @param mongoFieldName document key
     * @param g field getter
     * @param p optional field setter
     */
    class OptionalField[A](override val mongoFieldName: String, val g: T => Option[A], val p: Option[(T,Option[A]) => Unit])
            extends MongoScalar[A] with ScalarContent[A] with ScalarFieldModifyOp[A] with Optional[A] {
        override val rep = Represented.byOption(g, p)
        override def canEqual(other: Any): Boolean = other.isInstanceOf[OptionalField[_]]
    }

    /**
     * Shape object living in a field.
     *
     * For instantiation as an object: ObjectIn should be mixed in.
     * @param mongoFieldName document key
     * @param g field getter
     * @param p optional field setter
     */
    class EmbeddedField[V](override val mongoFieldName: String, val g: T => V, val p: Option[(T,V) => Unit])
            extends MongoScalar[V] with EmbeddedContent[V] with FieldModifyOp[V] {
        self: MongoField[V] with ObjectIn[V, QueryType] =>
        
        override val rep = parent.Represented.by(g, p)
        override def canEqual(other: Any): Boolean = other.isInstanceOf[EmbeddedField[_]]
    }

    /**
     * Reference
     * @param mongoFieldName document key
     * @param coll MongoCollection (typically ShapedCollection) where objects V live
     * @param g field getter
     * @param p optional field setter
     */
    class RefField[V <: MongoObject](override val mongoFieldName: String, override val coll: MongoCollection[V],
                                     val g: T => V, val p: Option[(T,V) => Unit])
            extends MongoScalar[V] with RefContent[V] {

        override val rep = parent.Represented.by(g, p)
        override def canEqual(other: Any): Boolean = other.isInstanceOf[RefField[_]]
    }

    /**
     * Optional reference
     * @param mongoFieldName document key
     * @param coll MongoCollection (typically ShapedCollection) where objects V live
     * @param g field getter
     * @param p optional field setter
     */
    class OptionalRefField[V <: MongoObject](override val mongoFieldName: String, override val coll: MongoCollection[V],
                                             val g: T => Option[V], val p: Option[(T,Option[V]) => Unit])
            extends MongoScalar[V] with RefContent[V] with Optional[V] {

        override val rep = parent.Represented.byOption(g, p)
        override def canEqual(other: Any): Boolean = other.isInstanceOf[OptionalRefField[_]]
    }

    /**
     * Array of scalars
     * @param mongoFieldName document key
     * @param g field getter
     * @param p optional field setter
     */
    class ArrayField[A](override val mongoFieldName: String, val g: T => Seq[A], val p: Option[(T,Seq[A]) => Unit])
            extends MongoArray[A] with ScalarContent[A] with ArrayFieldModifyOp[A] {

        override val rep = Represented.by[Seq[A]](g, p)
        override def canEqual(other: Any): Boolean = other.isInstanceOf[ArrayField[_]]
    }

    /**
     * Array of embedded objects. Must be subclassed: ObjectIn should be mixed in.
     * @param mongoFieldName document key
     * @param g field getter
     * @param p optional field setter
     */
    class ArrayEmbeddedField[V](override val mongoFieldName: String, val g: T => Seq[V], val p: Option[(T,Seq[V]) => Unit])
            extends MongoArray[V] with EmbeddedContent[V] with ArrayFieldModifyOp[V] {
        self: MongoField[V] with ObjectIn[V, QueryType] =>

        override val rep = parent.Represented.by(g, p)
        override def canEqual(other: Any): Boolean = other.isInstanceOf[ArrayEmbeddedField[_]]

        def where(q: QueryTerm[V]) = {
          import wrapper.MongoCondition._

          mkCond(elemMatch, Some(q.m))
        }
    }

    /**
     * Array of references
     * @param mongoFieldName document key
     * @param coll MongoCollection (typically ShapedCollection) where objects V live
     * @param g field getter
     * @param p optional field setter
     */
    class ArrayRefField[V <: MongoObject](override val mongoFieldName: String, override val coll: MongoCollection[V],
                                          val g: T => Seq[V], val p: Option[(T,Seq[V]) => Unit])
            extends MongoArray[V] with RefContent[V] {

        override val rep = parent.Represented.by(g, p)
        override def canEqual(other: Any): Boolean = other.isInstanceOf[ArrayRefField[_]]
    }

    /**
     * Map of scalars
     * @param mongoFieldName document key
     * @param g field getter
     * @param p optional field setter
     */
    class MapField[A](override val mongoFieldName: String,
                      val g: T => Map[String,A], val p: Option[(T,Map[String,A]) => Unit])
            extends MongoMap[A] with ScalarContent[A] { field =>

        def apply(key: String) = new ScalarField[A](key, (x: T) => g(x)(key), None) {
            override def mongoFieldPath = field.mongoFieldPath ::: super.mongoFieldPath
        }

        override val rep = Represented.by[Map[String,A]](g, p)
        override def canEqual(other: Any): Boolean = other.isInstanceOf[MapField[_]]
    }

    /**
     * Map of embedded objects. Must be subclassed: ObjectIn should be mixed in.
     * @param mongoFieldName document key
     * @param g field getter
     * @param p optional field setter
     */
    class MapEmbeddedField[V](override val mongoFieldName: String,
                              val g: T => Map[String,V], val p: Option[(T,Map[String,V]) => Unit])
            extends MongoMap[V] with EmbeddedContent[V] {
        self: MongoField[V] with ObjectIn[V, QueryType] =>

        override val rep = parent.Represented.by(g, p)
        override def canEqual(other: Any): Boolean = other.isInstanceOf[MapEmbeddedField[_]]
    }

    /**
     * Map of references
     * @param mongoFieldName document key
     * @param coll MongoCollection (typically ShapedCollection) where objects V live
     * @param g field getter
     * @param p optional field setter
     */
    class MapRefField[V <: MongoObject](override val mongoFieldName: String, override val coll: MongoCollection[V],
                                        val g: T => Map[String,V], val p: Option[(T,Map[String,V]) => Unit])
            extends MongoMap[V] with RefContent[V] {

        override val rep = parent.Represented.by(g, p)
        override def canEqual(other: Any): Boolean = other.isInstanceOf[MapRefField[_]]
    }

    /**
     * Factory methods to build pre-cooked field declarations
     */
    object Field {
        def scalar[A](fieldName: String, getter: T => A) =
            new ScalarField[A](fieldName, getter, None)

        def scalar[A](fieldName: String, getter: T => A, setter: (T, A) => Unit) =
            new ScalarField[A](fieldName, getter, Some(setter))

        def optional[A](fieldName: String, getter: T => Option[A]) =
            new OptionalField[A](fieldName, getter, None)

        def optional[A](fieldName: String, getter: T => Option[A], setter: (T, Option[A]) => Unit) =
            new OptionalField[A](fieldName, getter, Some(setter))

        def ref[V <: MongoObject](fieldName: String, coll: MongoCollection[V], getter: T => V) =
            new RefField[V](fieldName, coll, getter, None)

        def ref[V <: MongoObject](fieldName: String, coll: MongoCollection[V], getter: T => V, setter: (T, V) => Unit) =
            new RefField[V](fieldName, coll, getter, Some(setter))

        def optionalRef[V <: MongoObject](fieldName: String, coll: MongoCollection[V], getter: T => Option[V]) =
            new OptionalRefField[V](fieldName, coll, getter, None)
        
        def optionalRef[V <: MongoObject](fieldName: String, coll: MongoCollection[V], getter: T => Option[V], setter: (T, Option[V]) => Unit) =
            new OptionalRefField[V](fieldName, coll, getter, Some(setter))

        def array[A](fieldName: String, getter: T => Seq[A]) =
            new ArrayField[A](fieldName, getter, None)

        def array[A](fieldName: String, getter: T => Seq[A], setter: (T, Seq[A]) => Unit) =
            new ArrayField[A](fieldName, getter, Some(setter))

        def arrayRef[V <: MongoObject](fieldName: String, coll: MongoCollection[V], getter: T => Seq[V]) =
            new ArrayRefField[V](fieldName, coll, getter, None)

        def arrayRef[V <: MongoObject](fieldName: String, coll: MongoCollection[V], getter: T => Seq[V], setter: (T, Seq[V]) => Unit) =
            new ArrayRefField[V](fieldName, coll, getter, Some(setter))

        def map[A](fieldName: String, getter: T => Map[String,A]) =
            new MapField[A](fieldName, getter, None)

        def map[A](fieldName: String, getter: T => Map[String,A], setter: (T, Map[String,A]) => Unit) =
            new MapField[A](fieldName, getter, Some(setter))

        def mapRef[V <: MongoObject](fieldName: String, coll: MongoCollection[V], getter: T => Map[String,V]) =
            new MapRefField[V](fieldName, coll, getter, None)

        def mapRef[V <: MongoObject](fieldName: String, coll: MongoCollection[V], getter: T => Map[String,V], setter: (T, Map[String,V]) => Unit) =
            new MapRefField[V](fieldName, coll, getter, Some(setter))
    }

    /**
     * Optional field means it imposes no constraint
     */
    trait Optional[A] extends MongoField[A] { self: FieldContent[A] =>
        override def mongoConstraints = QueryTerm[QueryType]()
    }
}
