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

import org.specs._
import org.bson.types.ObjectId
import com.mongodb._

import Config._
import wrapper.DBO

object collectionSpec extends Specification("Scala way Mongo collections") {
    val mongo = new Mongo(Host, Port).getDB(Database)

    doAfter { mongo.dropDatabase }

    "Empty DBOCollection" should {
        val coll = mongo.getCollection("test1").asScala

        "have proper inheritance" in {
            coll must haveSuperClass[Iterable[DBObject]]
            coll must haveSuperClass[MongoCollection[DBObject]]
        }
        "support Iterable methods" in {
            coll.isEmpty must beTrue
            coll must beEmpty
        }
        "support Collection methods" in {
            coll must beEmpty
            coll.headOption must beNone
        }
    }
    "DBOCollection" should {
        val dbColl = mongo.getCollection("test")
        val coll = dbColl.asScala

        doAfter  { coll.drop }

        "be equal only when DBCollection equals" in {
            dbColl.asScala must be_==(coll)
            mongo.getCollection("test1").asScala must be_!=(coll)
        }
        "proxy to DBCollection" in {
            coll.getName must be_==(dbColl.getName)
            coll.getFullName must be_==(dbColl.getFullName)
        }
    }
    "DBOCollection" can {
        val dbColl = mongo.getCollection("test")
        val coll = dbColl.asScala

        doAfter  { coll.drop }

        "insert" in {
            coll must beEmpty
            ( coll << Map("key" -> 10) ).getLastError.ok must beTrue
            coll must haveSize(1)
            val dbo: DBObject = Map("key" -> 10)
            ( coll << dbo ).getLastError.ok must beTrue
            coll must haveSize(2)
            coll.headOption must beSome[DBObject].which{_.get("_id") != null}
        }
        "insert with oid check" in {
            coll must beEmpty
            val dbo = coll <<? Map("key" -> 10)
            dbo must beSome[DBObject].which {x =>
                x.get("_id") != null &&
                x.get("key") == 10
            }
            coll <<? Map("key" -> 10, "_id" -> dbo.get.get("_id")) must beNone
        }
        "save" in {
            coll must beEmpty
            coll += Map("key" -> 10)
            coll must haveSize(1)
            val dbo: DBObject = Map("key" -> 10)
            coll += dbo
            coll must haveSize(2)
            coll.headOption must beSome[DBObject].which{_.get("_id") != null}
        }
        "remove" in {
            coll must beEmpty
            val o: DBObject = Map("key" -> 10)
            coll += o
            coll must haveSize(1)
            coll -= o
            coll must beEmpty
        }
        "remove many elements" in {
          coll must beEmpty
          for {o <- List(Map("key" -> 10), Map("key" -> 15), Map("key" -> 20))}
            coll += o
          coll must haveSize(3)
          coll -= Map("key" -> Map("$lt" -> 20))
          coll must haveSize(1)
        }
        "insert many" in {
            val N = 20
            val objs = for {n <- 1 to N} yield DBO.fromMap(Map("key" -> n))
            ( coll << objs ).getLastError.ok must beTrue
            coll must haveSize(N)
            coll must haveTheSameElementsAs(objs)
        }
        "get by oid" in {
            coll must beEmpty
            val newO: DBObject = Map("key" -> 10)
            coll += newO
            coll must haveSize(1)
            
            val oid = newO.get("_id").asInstanceOf[ObjectId]
            coll.isDefinedAt(oid) must beTrue
            coll(oid) must be_==(newO)
        }
        "update" in {
            val q = Query(Map("key" -> 10))

            val N = 20
            for {n <- 1 to N} coll += Map("key" -> n)
            coll must haveSize(N)

            coll.update(q, Map("$inc" -> Map("key" -> N/2)), false)
            coll must haveSize(N)
            (q in coll).headOption must beNone
            (Query(Map("key" -> N)) in coll) must haveSize(2)
        }
        "update all" in {
            val N = 20
            for {n <- 1 to N} coll += Map("key" -> n)
            coll must haveSize(N)

            coll.update(Query(), Map("$inc" -> Map("key" -> -N/2)), true) must beTrue
            (Query(Map("key" -> Map("$gt" -> 0))) in coll) must haveSize(N/2)
            (Query(Map("key" -> Map("$lte" -> 0))) in coll) must haveSize(N/2)
        }
        "update none" in {
          coll must beEmpty
          coll.update(Query(), Map("$inc" -> Map("i" -> 1)), true) must beFalse
        }
        "findAndRemove" in {
            val N = 20
            val objs = for {n <- 1 to N} yield DBO.fromMap(Map("key" -> n))
            coll << objs
            coll must haveSize(N)

            coll.findAndRemove(Query(Map("i" -> "a"))) must beNone

            val q = Query( Map("key" -> Map("$gt" -> (N-1))) )
            coll.findAndRemove(q) must beSome[DBObject].which{dbo =>
              dbo.get("key") == N
            }
            coll must haveSize(N-1)
        }
        "findAndModify" in {
            val N = 20
            val objs = for {n <- 1 to N} yield DBO.fromMap(Map("key" -> n))
            coll << objs
            coll must haveSize(N)

            val q = Query(Map("key" -> Map("$gt" -> (N-1))))
            coll.findAndModify(q, Map("$inc" -> Map("key" -> 2))) must beSome[DBObject].which {dbo =>
              dbo.get("key") == N
            }
            coll must haveSize(N)
            coll exists {dbo => dbo.get("key") == N+2} must beTrue
        }
        "findAndModify upsert" in {
            val N = 20
            val objs = for {n <- 1 to N} yield DBO.fromMap(Map("key" -> n))
            coll << objs
            coll must haveSize(N)

            val q = Query(Map("key" -> (N+1)))
            val r = coll.findAndModify(q, Map("$inc" -> Map("key" -> 2)), false, true, true)
            r must beSome[DBObject].which {dbo =>
              dbo.get("key") == N+3
            }
            coll must haveSize(N+1)
        }
    }
}
