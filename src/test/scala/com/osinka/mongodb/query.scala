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
import com.mongodb._

import wrapper.DBO
import Config._

object querySpec extends Specification {
    val mongo = new Mongo(Host, Port).getDB(Database)

    doAfter { mongo.dropDatabase }

    "Query" should {
        "be empty initially" in {
            val q = Query()
            q.query must be_==(DBO.empty)
            q.skip must beNone
            q.limit must beNone
        }
        "store DBObject query" in {
            val q = Query(Map("a" -> 1), Some(1), Some(1), None)
            q.query.get("a") must (notBeNull and be_==(1))
            q.skip must beSome[Int].which{_ == 1}
            q.limit must beSome[Int].which{_ == 1}
        }
        "skip" in {
            val q = Query()
            (q drop None).skip must beNone
            (q drop 10 drop 2).skip must beSome[Int].which{_ == 2}
            (q drop 1 drop None).skip must beNone
        }
        "limit" in {
            val q = Query()
            (q take None).limit must beNone
            (q take 10 take 2).limit must beSome[Int].which{_ == 2}
            (q take 1 take None).limit must beNone
        }
        "() ++" in {
            val q = Query() ++ Map("a" -> 1)
            q.query.get("a") must (notBeNull and be_==(1))
            q.skip must beNone
            q.limit must beNone

        }
        "(a,s,l) ++" in {
            val q = Query(Map("a" -> 1), Some(1), Some(1), None) ++ Map("b" -> 1)
            q.query.get("a") must (notBeNull and be_==(1))
            q.query.get("b") must (notBeNull and be_==(1))
            q.skip must beSome[Int].which{_ == 1}
            q.limit must beSome[Int].which{_ == 1}
        }
        "++ with same key" in {
            val q = Query(Map("a" -> 1), Some(1), Some(1), None) ++ Map("a" -> 10)
            q.query.get("a") must (notBeNull and be_==(10))
            q.skip must beSome[Int].which{_ == 1}
            q.limit must beSome[Int].which{_ == 1}
        }
        "*" in {
            val q = Query(Map("a" -> 1, "b" -> 2), Some(1), Some(2), Some(Map("a" -> 1))) *
                    Query(Map("b" -> 10, "c" -> 3), Some(2), Some(5), Some(Map("a" -> 2)))
            q.query.get("a") must (notBeNull and be_==(1))
            q.query.get("b") must (notBeNull and be_==(10))
            q.query.get("c") must (notBeNull and be_==(3))
            q.skip must beSome[Int].which{_ == 2}
            q.limit must beSome[Int].which{_ == 5}
            q.sorting must beSome[DBObject].which{_.get("a") == 2}
        }
    }

    "Query(coll)" should {
        val coll = mongo.getCollection("test").asScala

        doFirst {
            coll.drop
            Helper.fillWith(coll, 5) {i => Map("a" -> i)}
        }
        doLast  {
            coll.drop
        }

        "support DSL" in {
            val q = Query() drop 1 take 2
            q must be_==( Query(DBO.empty, Some(1), Some(2), None) )
            q.sort(Map("a" -> 1)).sorting must beSome[DBObject].which{_.get("a") == 1}
            (q in coll).query must be_==(q)
        }
        "apply to DBObjectCollection" in {
            val c = Query() in coll
            c must haveSuperClass[DBObjectCollection]
            (Query() in c) must haveSuperClass[DBObjectCollection]
            c must haveSize(5)
            c.iterator.toSeq must haveSize(5)
        }
        "sort ascending" in {
            val c = Query() sort Map("a" -> 1) in coll
            c.query.sorting must beSome[DBObject].which{_.get("a") == 1}
            c.headOption must beSome[DBObject].which{_.get("a") == 0}
        }
        "sort descending" in {
            val c = Query() sort Map("a" -> -1) in coll
            c must haveSuperClass[DBObjectCollection]
            c.query.sorting must beSome[DBObject].which{_.get("a") == -1}
            c.headOption must beSome[DBObject].which{_.get("a") == 4}
        }
        "support skip" in {
            (Query() drop 1 in coll).iterator.toSeq must haveSize(4)
            Query() drop 1 in coll must haveSize(4)
            (Query() drop 1 in coll).iterator.toSeq must haveSize(4)
            Query() drop 1 drop 1 in coll must haveSize(4)
            Query() drop 5 in coll must beEmpty
            Query() drop 6 in coll must haveSize(0)
            (Query() drop 6 in coll).iterator.toSeq must beEmpty
        }
        "support limit" in {
            Query() take 1 in coll must haveSize(1)
            Query() drop 1 take 2 in coll must haveSize(2)
            Query() take 2 drop 1 in coll must haveSize(2)
            (Query() take 2 drop 1 in coll).iterator.toSeq must haveSize(2)
        }
    }
}
