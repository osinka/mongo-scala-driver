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

import com.mongodb._
import com.osinka.mongodb._

// case class Model with constant field, its extractor and factory method
case class CaseUser(val name: String) extends MongoObject {
    override def toString = "CaseUser(name="+name+",oid="+mongoOID+")"
}

trait CaseUserIn[T] extends ObjectIn[CaseUser, T] {
    object name extends ScalarField[String]("name", _.name, None)
    override lazy val * = name :: Nil
    override def factory(dbo: Option[DBObject]): Option[CaseUser] = for {name(n) <- dbo} yield new CaseUser(n)
}

object CaseUser extends MongoObjectShape[CaseUser] with CaseUserIn[CaseUser]

// ordinary class model with variable and updatable field
class OrdUser extends MongoObject {
    var name: String = _
    override def toString = "OrdUser(name="+name+",oid="+mongoOID+")"
}
object OrdUser extends MongoObjectShape[OrdUser] {
    override def factory(dbo: Option[DBObject]) = Some(new OrdUser)

    lazy val name = Field.scalar("name",
           (u: OrdUser) => u.name,
           (u: OrdUser, n: String) => u.name = n)

    override lazy val * = name :: Nil
}

// object holder for serializer tests
case class Holder[T](var value: T)

class TSerializer[T](val f: () => Holder[T]) extends ObjectShape[Holder[T]] with FunctionalShape[Holder[T]] {
    lazy val i = Field.scalar("i", (x: Holder[T]) => x.value, (x: Holder[T], v: T) => x.value = v)

    override lazy val * = List(i)
    override def factory(dbo: Option[DBObject]): Option[Holder[T]] = Some(f())
}
