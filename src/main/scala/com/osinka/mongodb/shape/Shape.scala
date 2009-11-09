package com.osinka.mongodb.shape

import scala.reflect.Manifest
import com.mongodb._
import Helper._

/*
 * Basic object shape
 */
trait BaseShape[+S] {
    val shape: S
}

/*
 * Shape of an object backed by DBObject ("hosted in")
 */
trait DBObjectShape[T] extends Transformer[T, DBObject] with BaseShape[DBObject] with ShapeFields[T, T] with Queriable[T] {
    def * : List[Field[T, _, _]]
    def factory(dbo: DBObject): Option[T]

    // -- BaseShape[S]
    lazy val shape: DBObject =
        Preamble.createDBObject(
            (* remove {_.mongo_?} foldLeft Map[String,Any]() ) { (m,f) =>
                assert(f != null, "Field must not be null")
                m + (f.fieldName -> f.shape.ensuring(_!=null, "Field "+f+" should not have null shape"))
            }
        )

    // -- Transformer[T, R]
    override def extract(dbo: DBObject) = factory(dbo) map { x =>
        assert(x != null, "Factory should not return Some(null)")
        for {val f <- * if f.isInstanceOf[HostUpdate[_,_]]
             val fieldDbo <- tryo(dbo.get(f.fieldName))}
            f.asInstanceOf[HostUpdate[T,_]].updateUntyped(x, fieldDbo)
        x
    }

    override def pack(x: T): DBObject =
        Preamble.createDBObject(
            (* foldLeft Map[String,Any]() ) { (m,f) =>
                assert(f != null, "Field must not be null")
                m + (f.fieldName -> f.valueOf(x))
            }
        )
}

/**
 * Mix-in to make a shape functional, see FunctionalTransformer for explanation
 *
 * ShapeFunctionObject will provide shape with convinience syntactic sugar
 * for converting object to DBObject and extractor for opposite
 *
 * E.g.
 * val dbo = UserShape(u)
 * dbo match {
 *    case UserShape(u) =>
 * }
 *
 * The same applies to field shapes
 */
trait FunctionalShape[T] { self: DBObjectShape[T] =>
    def apply(x: T): DBObject = pack(x)
    def unapply(rep: DBObject): Option[T] = extract(rep)
}

/**
 * Shape of MongoObject child.
 *
 * It has mandatory _id and _ns fields
 */
trait MongoObjectShape[T <: MongoObject] extends DBObjectShape[T] {
    object oid extends Scalar[ObjectId]("_id", _.mongoOID)
            with Functional[ObjectId]
            with Mongo[ObjectId]
            with Updatable[ObjectId] {
        override def update(x: T, oid: ObjectId): Unit = x.mongoOID = oid
    }
    object ns extends Scalar[String]("_ns", _.mongoNS)
            with Functional[String]
            with Mongo[String]
            with Updatable[String] {
        override def update(x: T, ns: String): Unit = x.mongoNS = ns
    }

    // -- DBObjectShape[T]
    override def * : List[Field[T, _, _]] = oid :: ns :: Nil
}

/*
 * Shape to be used by users.
 */
trait Shape[T <: MongoObject] extends MongoObjectShape[T]

abstract class AbstractShape[T <: MongoObject](implicit m: Manifest[T]) extends Shape[T] {
    val clazz = m.erasure

    // -- DBObjectShape[T]
    override def factory(dbo: DBObject): Option[T] = Some(clazz.newInstance.asInstanceOf[T])
}