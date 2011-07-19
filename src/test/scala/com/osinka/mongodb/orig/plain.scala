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
package com.osinka.mongodb.orig

import org.specs._
import com.mongodb._

import com.osinka.mongodb._
import Config._

object plainSpec extends Specification {
    val mongo = new Mongo(Host, Port).getDB(Database)

    doAfter  { mongo.dropDatabase }

    "DBObject" should {
        "copy pattern over" in {
            import java.util.regex.Pattern

            val re = Pattern.compile("^re.*")
            val dbo = new BasicDBObject
            dbo putAll BasicDBObjectBuilder.start("a", 3).get
            dbo putAll BasicDBObjectBuilder.start("a", re).get
            dbo.get("a") must (notBeNull and be_==(re))
        }
    }
    "Plain collection" should {
        val coll = mongo.getCollection("test")

        doBefore { coll.drop }
        doAfter  { coll.drop }

        "have zero size by default" in {
            coll.getCount must be_==(0)
        }
        "accept DBObjects" in {
            coll.getCount must be_==(0)
            coll insert BasicDBObjectBuilder.start("key", 100).get
            coll.getCount must be_==(1)
        }
        "return same DBObject" in {
            val dbo = BasicDBObjectBuilder.start("key", 100).get
            coll save dbo
            coll.getCount must be_==(1)

            val o = coll.findOne
            o must be_==(dbo)
            o.get("key") must be_==(100)
        }
        "remove DBObjects by object" in {
            val o = BasicDBObjectBuilder.start("key", 100).get
            coll save o
            coll.getCount must be_==(1)

            coll.remove(o)
        }
        "'save' should replace object" in {
            coll.getCount must be_==(0)

            val o = BasicDBObjectBuilder.start("key", 100).get
            coll save o
            coll.getCount must be_==(1)

            o.put("key", 200)
            coll save o
            coll.getCount must be_==(1)

            val r = coll.findOne
            r must notBeNull
            r.get("key") must be_==(200)
        }
        "not insert duplicate id" in {
            coll.getCount must be_==(0)

            val o = BasicDBObjectBuilder.start("key", 100).get
            coll save o
            coll.getCount must be_==(1)

            o.put("key", 200)
            coll insert o
            mongo.getLastError aka "error on duplicate key insert" must notBeNull
            
            coll.getCount must be_==(1)
            val r = coll.findOne
            r must notBeNull
            r.get("key") must be_==(100)
        }
        "group" in {
            skip("TODO: group spec")
        }
    }
//    "Index" should {
//        skip("TODO: indexing spec")
//    }
    "Query" should {
        val coll = mongo.getCollection("test")

        doBefore { coll.drop }
        doAfter  { coll.drop }

        "count by query" in {
            coll save BasicDBObjectBuilder.start("a", "value").get
            coll getCount BasicDBObjectBuilder.start("a", "value").get must be_==(1)

            coll save BasicDBObjectBuilder.start(
                "a", BasicDBObjectBuilder.start("b", "other").get
            ).get
            coll getCount BasicDBObjectBuilder.start("a.b", "other").get must be_==(1)
            coll getCount BasicDBObjectBuilder.start(
                "a", BasicDBObjectBuilder.start("b", "other").get
            ).get must be_==(1)
        }
        "count by query and shape" in {
            coll save BasicDBObjectBuilder
                .start("a", "value")
                .get
            coll save BasicDBObjectBuilder
                .start("a", "value")
                .push("b")
                    .append("c", "other")
                .get
            coll.find(BasicDBObjectBuilder.start.push("b.c").append("$exists", true).get).count must be_==(1)
//            coll.getCount(BasicDBObjectBuilder.start("a", "value").get,
//                          BasicDBObjectBuilder.start.push("b").append("c", 1).get
//            ) must be_==(1)
        }
        "regexp" in {
            import java.util.regex.Pattern

            val reQuery = BasicDBObjectBuilder.start("a", Pattern.compile(".*es.*")).get
            val shape = BasicDBObjectBuilder.start("a", 1).get

            doBefore { coll save BasicDBObjectBuilder.start("a", "test").get }
            doAfter {}

            "getCount" in {
                coll.getCount(reQuery) must be_==(1)
                coll.getCount(reQuery, shape) must be_==(1)
            }
            "findOne" in {
                coll.findOne(reQuery) must notBeNull
                coll.findOne(reQuery, shape) must notBeNull
            }
            "find" in {
                coll.find(reQuery).count must be_==(1)
                coll.find(reQuery, shape).count must be_==(1)
            }
        }
        "update by query" in {
            skip("TODO: update")
        }
        "lookup by query" in {
            skip("TODO: lookup")
        }
        "remove DBObjects by query" in {
            val o = BasicDBObjectBuilder.start("key", 100).get
            coll save o
            coll.getCount must be_==(1)

            coll.remove(o)
            coll.getCount must be_==(0)
        }
    }
    "DBCursor" should {
        val coll = mongo.getCollection("test")

        def collection(f: (DBCursor => DBCursor)) = new wrapper.DBObjectIterator(f(coll.find)).toSeq
        def count(f: (DBCursor => DBCursor)) = f(coll.find).count

        doFirst {
            coll.drop
            def gen(n: Int) = Array.tabulate(n) { i => Map("a" -> ("a"+i) ) }
            for (o <- gen(5)) coll save o
        }
        doLast  {
            coll.drop
        }

        "count" in {
            coll.getCount must be_==(5)
            coll.find.count must be_==(5)

            collection(x => x) must haveSize(5)
            count(x => x) must be_==(5)
        }
        "count regexp" in {
            import java.util.regex.Pattern
            import Pattern._

            val q = Map("a" -> Pattern.compile("a3$", CASE_INSENSITIVE))
            coll.find(q).count must be_==(1)
        }
        "limit" in {
            collection(_ limit -1) mustNot haveSize(5)
            count(_ limit -1) must be_==(5)
            
            collection(_ limit 2) must haveSize(2)
            count(_ limit 2) must be_==(5)
        }
        "skip" in {
            collection(_ skip 0) must haveSize(5)
            count(_ skip 0) must be_==(5)

            collection(_ skip 1) must haveSize(4)
            count(_ skip 1) must be_==(5)

            collection(_ skip 1 limit 2) must haveSize(2)
            count(_ skip 1 limit 2) must be_==(5)
        }
        "sort ascending" in {
            coll.find.count must be_==(5)

            val c = coll.find.sort(Map("a" -> 1)).limit(1)
            c.hasNext must beTrue

            val o = c.next
            o must notBeNull
            o.get("a") must be_==("a0")
        }
        "sort descending" in {
            val c = coll.find.sort(Map("a" -> -1)).limit(1)
            c.hasNext must beTrue

            val o = c.next
            o must notBeNull
            o.get("a") must be_==("a4")
        }
    }
    "DBRef" should {
        val coll = mongo.getCollection("test")

        setSequential
        doFirst { coll.drop }
        doLast  { coll.drop }

        "store and fetch" in {
            val subobj: DBObject = Map("s" -> "other things", "num" -> 100)
            coll save subobj
            subobj.get("_id") must notBeNull

            val ref = new DBRef(coll.getDB, "test", subobj.get("_id"))

            val obj: DBObject = Map("object" -> "complex", "sub" -> ref)
            coll save obj
            obj.get("_id") must notBeNull

            obj.get("sub") must haveSuperClass[DBRefBase]
            val deref = obj.get("sub").asInstanceOf[DBRefBase]
            val deSubObj = deref.fetch
            deSubObj must notBeNull
            deSubObj.get("_id") must be_==(subobj.get("_id"))
        }
    }
    "Types save/retrieve" should {
        val coll = mongo.getCollection("test")

        doBefore { coll.drop }
        doAfter  { coll.drop }

        "work out Strings" in {
            val dbo = BasicDBObjectBuilder.start("a", "val").get
            dbo.get("a") must haveClass[String]

            coll save dbo
            val res = coll.findOne.get("a")
            res must (haveClass[String] and be_==("val"))
        }
        "work out Ints" in {
            val dbo = BasicDBObjectBuilder.start("a", 1).get
            dbo.get("a") must haveClass[java.lang.Integer]

            coll.save(dbo)
            val res = coll.findOne.get("a")
            res must (haveClass[java.lang.Integer] and be_==(1))
        }
        "work out large Ints" in {
            val dbo = BasicDBObjectBuilder.start("a", 1000000).get
            dbo.get("a") must haveClass[java.lang.Integer]

            coll save dbo
            val res = coll.findOne.get("a")
            res must (haveClass[java.lang.Integer] and be_==(1000000))
        }
        "work out Longs" in {
            val dbo = BasicDBObjectBuilder.start("a", 1L).get
            dbo.get("a") must haveClass[java.lang.Long]

            coll save dbo
            val res = coll.findOne.get("a")
            res must (haveClass[java.lang.Long] and be_==(1L))
        }
        "work out Floats" in {
            val dbo = BasicDBObjectBuilder.start("a", 1.0F).get
            dbo.get("a") must haveClass[java.lang.Float]

            coll save dbo
            val res = coll.findOne.get("a")
            res aka "getting Float out from DBColl will return Double" must (haveClass[java.lang.Double] and be_==(1.0D))
        }
        "work out Doubles" in {
            val dbo = BasicDBObjectBuilder.start("a", 1.0D).get
            dbo.get("a") must haveClass[java.lang.Double]

            coll save dbo
            val res = coll.findOne.get("a")
            res must (haveClass[java.lang.Double] and be_==(1.0D))
        }
    }
    "DBObject serialization" should {
        "create DBO from Map" in {
            import scala.collection.JavaConversions._
            val m = scala.collection.mutable.Map[String, Any]()
            m += ("a" -> 1, "b" -> 2)

            val juMap: java.util.Map[String,Any] = m
            val dbo = BasicDBObjectBuilder.start(juMap).get
            dbo.containsField("a") must beTrue
            dbo.get("a") must be_==(1)
            dbo.containsField("b") must beTrue
            dbo.get("b") must be_==(2)
        }
        "convert Map of Arrays to DBO" in {
            skip("BasicDBObjectBuilder.start(Map) and BasicDBObject.putAll(Map) do not descend, they assume all values to be scalars")

            import scala.collection.JavaConversions._
            val m = scala.collection.mutable.Map[String, Any]()
            val a = Array[String]("v1", "v2")
            m += "c" -> a

            val juMap: java.util.Map[String,Any] = m
            val dbo = BasicDBObjectBuilder.start(juMap).get
            dbo.containsField("c") must beTrue
            dbo.get("c") must haveSuperClass[DBObject]
            val adbo = dbo.get("c").asInstanceOf[DBObject]
            adbo.get("0") must be_==("v1")
            adbo.get("1") must be_==("v2")
        }
    }
}
