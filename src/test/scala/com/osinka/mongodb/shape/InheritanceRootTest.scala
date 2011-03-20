package com.osinka.mongodb
package shape

import com.mongodb.{Mongo, DBObject}

/**
 * @author Dmitry Dobrynin
 * Date: 19.03.11 time: 1:42
 */
object InheritanceRootTest extends Application {
  val m = new Mongo
  val db = m.getDB("test")
  val entities = db.getCollection("entities") of AbstractEntity

//  entities << InheritedEntity1("InheritedEntity1: f1", "base1")
//  entities << InheritedEntity2("InheritedEntity2: f2", 555, "base2")

  for (e <- entities) println(e)

  val ie2 = db getCollection "entities" of InheritedEntity2Shape

  println("Find only inherited entities 2")
  for (e <- ie2) println(e)
}

class AbstractEntity(val base: String) extends MongoObject
case class InheritedEntity1(f1: String, b: String) extends AbstractEntity(b)
case class InheritedEntity2(f2: String, i: Int, b: String) extends AbstractEntity(b)

trait AbstractEntityShape[T <: AbstractEntity] extends EntityType[T] {
  val base = Field.scalar("base", _.base)
  def * : List[MongoField[_]] = List(base)
}

object InheritedEntity1Shape extends AbstractEntityShape[InheritedEntity1] {
  val f1 = Field.scalar("f1", _.f1)
  override def * = f1 :: super.*
  def factory(dbo: Option[DBObject]) = for (f1(f) <- dbo; base(b) <- dbo) yield InheritedEntity1(f, b)
}

object InheritedEntity2Shape extends AbstractEntityShape[InheritedEntity2] {
  val f2 = Field.scalar("f2", _.f2)
  val i = Field.scalar("i", _.i)
  override def * = f2 :: i :: super.*
  def factory(dbo: Option[DBObject]) = for (f2(f) <- dbo; i(i) <- dbo; base(b) <- dbo) yield InheritedEntity2(f, i, b)
}

object AbstractEntity extends InheritanceRootCompanion[AbstractEntity] {
  def descendants = InheritedEntity2Shape :: InheritedEntity1Shape :: Nil
}

