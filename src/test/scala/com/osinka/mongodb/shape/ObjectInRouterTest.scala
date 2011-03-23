package com.osinka.mongodb
package shape

import com.mongodb._

object ObjectInRouterTest extends Application {
  val m = new Mongo
  val db = m.getDB("test")
  val containers = db.getCollection("containers") of ContainerShape
  containers << Container(List(Embedded1("Embedded1: f1", "base 1"), Embedded2("Embedded2: f2", "base 2")))
  for (c <- containers) println(c)
}

case class Container(embedded: Seq[Embedded]) extends MongoObject

object ContainerShape extends MongoObjectShape[Container] {
  val embedded = new ArrayEmbeddedField("embedded", _.embedded, None) with ObjectInRouter[Embedded, Container] {
    this :=> Embedded1Shape :=> Embedded2Shape
  }
  def * = embedded :: Nil
  def factory(dbo: Option[DBObject]) = for (embedded(embedded) <- dbo) yield Container(embedded)
}

abstract class Embedded(val f: String)
case class Embedded1(f1: String, fb: String) extends Embedded(fb)
case class Embedded2(f2: String, fb: String) extends Embedded(fb)

trait EmbeddedShape[T <: Embedded] extends ObjectIn[T, T] {
  val f = Field.scalar("f", _.f)
  def * = f :: Nil
}

object Embedded1Shape extends EmbeddedShape[Embedded1] {
  val f1 = Field.scalar("f1", _.f1)
  override def * = f1 :: super.*
  def factory(dbo: Option[DBObject]) = for (f(f) <- dbo; f1(f1) <- dbo) yield Embedded1(f1, f)
}

object Embedded2Shape extends EmbeddedShape[Embedded2] {
  val f2 = Field.scalar("f2", _.f2)
  override def * = f2 :: super.*
  def factory(dbo: Option[DBObject]) = for (f(f) <- dbo; f2(f2) <- dbo) yield Embedded2(f2, f)
}