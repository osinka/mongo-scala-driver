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

import org.specs._
import java.util.Date
import org.bson.types.ObjectId
import com.mongodb.{DBObject, BasicDBObjectBuilder}

import com.osinka.mongodb._
import wrapper.DBO

object serializerSpec extends Specification {
    val Const = "John Doe"
    val jd = DBO.fromMap( Map("name" -> Const) )

    "Shape scalars" should {
        object IntS extends TSerializer[Int]( () => Holder[Int](99) )
        object StringS extends TSerializer[String]( () => Holder[String]("init") )
        object DateS extends TSerializer[Date]( () => Holder[Date](new Date))

        "serialize AnyVals" in {
            IntS.i.mongoReadFrom(Holder[Int](1)) must be_==( Some(1) )

            val h = Holder[Int](10)
            IntS.i.mongoWriteTo(h, Some(1))
            h.value must be_==(1)
        }
        "serialize Ints" in {
            BasicDBObjectBuilder.start("i", 1).get must beLike {
                case IntS(o) => o.value == 1
            }

            val dbo2 = IntS( Holder[Int](1) )
            dbo2.get("i") must (notBeNull and be_==(1))
        }
        "serialize Strings" in {
            BasicDBObjectBuilder.start("i", "test").get must beLike {
                case StringS(o) => o.value == "test"
            }

            val dbo2 = StringS( Holder[String]("test") )
            dbo2.get("i") must (notBeNull and be_==("test"))
        }
        "serialize Dates" in {
            BasicDBObjectBuilder.start("i", new Date(1)).get must beLike {
                case DateS(o) => o.value == new Date(1)
            }

            val dbo2 = DateS( Holder[Date](new Date(1)) )
            dbo2.get("i") must (notBeNull and be_==(new Date(1)))
        }
        "serialize Maps" in {
            skip("not implemented")
        }
        "serialize Arrays" in {
            skip("not implemented")
        }
    }
    "Case class Shape" should {
         val jd = DBO.fromMap( Map("name" -> Const) )

        "serialize to DBObject" in {
            val dbo = CaseUser in new CaseUser(Const)
            dbo must notBeNull
            dbo.get("name") must be_==(Const)
        }
        "serialize from DBObject" in {
            CaseUser.out(jd) must beSome[CaseUser].which{_.name == Const}
        }
        "not include _id and _ns into DBO" in {
            val shape = CaseUser.constraints
            shape must haveSuperClass[QueryTerm[CaseUser]]
            shape.m.get("name") must beSome[Any].which{_ == Map("$exists" -> true)}
            shape.m.get("_id") must beNone
            shape.m.get("_ns") must beNone
        }
        "mirror mongo fields back to object" in {
            import org.bson.types.ObjectId

            val dbo = DBO.empty
            dbo.putAll(jd)

            val u = CaseUser out dbo
            u must beSome[CaseUser]

            val user = u.get
            u.get must verify { user => user.name == Const && user.mongoOID == None}

            dbo.put("_id", ObjectId.get)
            CaseUser.mirror(user)(dbo)
            user.mongoOID must beSome[ObjectId].which{dbo.get("_id") ==}
        }
    }
    "Ordinary class Shape" should {
        "serialize to DBObject" in {
            val u = new OrdUser
            u.name = Const
            val dbo = OrdUser in u
            dbo.get("name") must be_==(Const)
        }
        "deserialize from DBObject" in {
            OrdUser.out(jd) must beSome[OrdUser].which{_.name == Const}
        }
    }
    "Class with Embedded object Shape" should {
        "serialize  to DBObject" in {
            val dbo = ComplexType in new ComplexType(CaseUser(Const), 1)
            dbo.get("user") must haveSuperClass[DBObject]
            dbo.get("user").asInstanceOf[DBObject].get("name") must be_==(Const)
            dbo.get("msgs") must haveClass[java.lang.Integer]
            dbo.get("msgs") must be_==(1)
        }
        "deserialize from DBObject" in {
            DBO.fromMap( Map("user" -> jd, "msgs" -> 1) ) match {
                case ComplexType(c) =>
                    c.user must notBeNull
                    c.user.name must be_==(Const)
                    c.messageCount must (notBeNull and be_==(1))
                case _ =>
                    fail("had to extract ComplexType out from DBO")
            }
        }
    }
    "Optional field" should {
        "have empty constraints" in {
            OptModel.description.mongoConstraints.m must beEmpty
            OptModel.description3.mongoConstraints.m must beEmpty
            OptModel.comment.mongoConstraints.m must beEmpty
        }
        "serialize to DBObject" in {
            val some = new OptModel(1, Some(Const))
            val none = new OptModel(1, None)
            OptModel.description.mongoReadFrom(none) must beNone
            OptModel.description.mongoReadFrom(some) must be_==(Some(Const))
            OptModel.description3.mongoReadFrom(none) must beNone
            OptModel.description3.mongoReadFrom(some) must be_==(Some(Const))
        }
        "deserialize from DBObject" in {
            val t = new OptModel(1, None)
            OptModel.comment.mongoWriteTo(t, Some("aa"))
            t.comment must be_==(Some("aa"))

            OptModel.comment.mongoWriteTo(t, None)
            t.comment must beNone
        }
    }
    "Query" should {
        "serialize two conditions per field" in {
            val q = (ComplexType.messageCount is_< 2) and (ComplexType.messageCount is_> 3)
            q.query.query must be_==( DBO.fromMap(
                    Map(ComplexType.messageCount.longFieldName -> Map("$lt" -> 2, "$gt" -> 3) )
            ) )
        }
    }
    "Modifiers" should {
        "serialize $set" in {
            (ComplexType.user.name set "User2").query.query must be_==(
                DBO.fromMap(
                    Map("$set" -> Map(
                            ComplexType.user.name.longFieldName -> "User2"
                        ) )
                )
            )
        }
        "serialize $set embedded" in {
            (ComplexType.user set CaseUser("User0")).query.query must be_==(
                DBO.fromMap(
                    Map("$set" -> Map(
                            ComplexType.user.longFieldName -> Map(CaseUser.name.longFieldName -> "User0")
                        ) )
                )
            )
        }
        "serialize $push" in {
            import ArrayOfInt._
            (ArrayModel.messages push 10).query.query must be_==(
                DBO.fromMap(
                    Map("$push" -> Map(ArrayModel.messages.longFieldName -> 10))
                )
            )
        }
        "serialize join" in {
            ((ComplexType.messageCount inc 10) and (ComplexType.user.name set "User1")).query.query must be_==(
                DBO.fromMap(
                    Map("$set" -> Map( ComplexType.user.name.longFieldName -> "User1" ),
                        "$inc" -> Map( ComplexType.messageCount.longFieldName -> 10 ) )
                )
            )
        }
        "serialize two field set" in {
            ((ComplexType.messageCount set 10) and (ComplexType.user.name set "User1")).query.query must be_==(
                DBO.fromMap(
                    Map("$set" -> Map( ComplexType.user.name.longFieldName -> "User1",
                                       ComplexType.messageCount.longFieldName -> 10 ) )
                )
            )
        }
    }
}
