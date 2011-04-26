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

import org.specs._
import org.bson.types.ObjectId
import com.mongodb._

import org.bson.types.ObjectId
import com.osinka.mongodb._
import Config._

object querySpec extends Specification("Query on Shapes and Fields") {
    val CollName = "test"
    val Const = "John Doe"

    val mongo = new Mongo(Host, Port).getDB(Database)

    doAfter { mongo.dropDatabase }

    "Field conditions" should {
        "on scalar fields" in {
            import java.util.regex.Pattern
            import Pattern._

            val scalaR = "^test$".r
            val javaR = Pattern.compile("^test$")

            CaseUserShape.name eq_? Const must be_==( QueryTerm( Map("name" -> Const) ) )
            CaseUserShape.name is_< Const must be_==( QueryTerm( Map("name" -> Map("$lt" -> Const))) )
            CaseUserShape.name is_<= Const must be_==( QueryTerm( Map("name" -> Map("$lte" -> Const))) )
            CaseUserShape.name is_> Const must be_==( QueryTerm( Map("name" -> Map("$gt" -> Const))) )
            CaseUserShape.name is_>= Const must be_==( QueryTerm( Map("name" -> Map("$gte" -> Const))) )
            CaseUserShape.name in List(Const) must be_==( QueryTerm( Map("name" -> Map("$in" -> List(Const)))) )
            CaseUserShape.name.exists  must be_==( QueryTerm( Map("name" -> Map("$exists" -> true))) )
            (CaseUserShape.name is_~ scalaR).m.get("name") must beLike {
                case Some(p: Pattern) => p.pattern == javaR.pattern
            }
        }
        "on embedded map fields" in {
            import MapOfEmbedded._

            MapModel.users("a").exists must be_==( QueryTerm( Map("users.a" -> Map("$exists" -> true)) ) )
            MapModel.users("a").name is_== Const must be_==( QueryTerm( Map("users.a.name" -> Const) ) )
        }
        "on array of embedded documents" in {
            import ArrayOfEmbedded._

            ArrayModel.users.where(CaseUserShape.name is_== Const) must be_==( QueryTerm( Map("users" -> Map("$elemMatch" -> Map("name" -> Const)) ) ))
        }
    }
    "Shape query" should {
        "have DSL" in {
            val qt = CaseUserShape.name is_== Const
            qt must haveSuperClass[QueryTerm[CaseUser]]
            qt must be_==( QueryTerm[CaseUser]( Map("name" -> Const)) )

            val q = CaseUserShape where {CaseUserShape.name is_< Const} drop 10 take 10 sortBy CaseUserShape.name.ascending
            q must haveSuperClass[ObjectShape[CaseUser]#ShapeQuery]
            q.query must be_==( Query(Map("name" -> Map("$lt" -> Const)), Some(10), Some(10), Some(Map("name" -> 1))) )

            (CaseUserShape sortBy CaseUserShape.name.descending).query.sorting must beSome[DBObject].which{_.get("name") == -1}
        }
        "produce right DBO for regex query" in {
            import java.util.regex.Pattern
            val qt = CaseUserShape.name is_~ "^User3$".r
            val dboRE = CaseUserShape.where(qt).query.query.get("name")
            dboRE must (notBeNull and beLike {
                case p: Pattern => p.pattern == "^User3$"
            })
        }
    }
    "Query" should {
        val dbColl = mongo.getCollection(CollName)
        val coll = dbColl of CaseUserShape
        val N = 50

        doBefore {
            dbColl.drop
            Helper.fillWith(coll, N) {x => CaseUser("User"+x)}
        }
        doAfter {
            dbColl.drop
        }

        "retain coll type" in {
            coll applied Query() must haveSuperClass[ShapedCollection[CaseUser]]
        }
        "support skip/limit" in {
            coll must haveSize(N)
            coll applied (Query() take 1) must haveSize(1)
            coll applied (Query() drop 10 take 5) must haveSize(5)
            coll applied (Query() drop N-5 take 10) must haveSize(5)
        }
        "ignore different shape" in {
            val cmplxColl = dbColl of ComplexType
            cmplxColl must beEmpty
            cmplxColl.iterator.toSeq must beEmpty
        }
        "do find" in {
            val r = coll applied Query(Map(CaseUserShape.name.mongoFieldName -> "User2"))
            r must haveSize(1)
            r must contain( CaseUser("User2") )
        }
        "do headOption" in {
            val r = coll applied Query(Map(CaseUserShape.name.mongoFieldName -> "User2"))
            r must haveSize(1)
            r.headOption must beSome[CaseUser].which{_.name == "User2"}

            (coll applied Query(Map("a" -> 1))).headOption must beNone
        }
        "apply ==" in {
            val c = CaseUserShape where {CaseUserShape.name is "User3"} take 1 in coll
            c must haveSize(1)
        }
        "apply <" in {
            CaseUserShape where {CaseUserShape.name is_< "User3"} in coll must haveSize(23)
        }
        "apply ~" in {
            import java.util.regex.Pattern
            import Pattern._

            CaseUserShape.name is_~ Pattern.compile("user3$", CASE_INSENSITIVE) in coll must haveSize(1)
            CaseUserShape.name like "^User3$".r in coll must haveSize(1)
        }
        "remove" in {
            coll -= (CaseUserShape where {CaseUserShape.name is_~ "^User3.$".r} )
            coll must haveSize(N-10)
        }
        "sort ascending" in {
            val c = CaseUserShape sortBy CaseUserShape.name.ascending take 1 in coll
            c must haveSize(1)
            c.headOption must beSome[CaseUser].which{_.name == "User0"}
        }
        "sort descending" in {
            val c = CaseUserShape sortBy CaseUserShape.name.descending take 1 in coll
            c must haveSize(1)
            c.headOption must beSome[CaseUser].which{_.name == "User9"}
        }
        "sort by two fields" in {
            skip("not implemented")
        }
    }
    "Query embedded" should {
        val dbColl = mongo.getCollection(CollName)
        val coll = dbColl of ComplexType
        val N = 50

        doFirst {
            dbColl.drop
            Helper.fillWith(coll, N) {x => new ComplexType(CaseUser("User"+x), x*10)}
        }
        doLast {
            dbColl.drop
        }

        "apply ==" in {
            val c = ComplexType where {ComplexType.user.name eq_? "User3"} take 1 in coll
            c must haveSize(1)

            val s = c.toSeq
            s(0).user must be_==( CaseUser("User3") )
            s(0).messageCount must be_==(30)
        }
        "apply <" in {
            ComplexType where {ComplexType.user.name is_< "User3"} in coll must haveSize(23)
        }
        "apply in" in {
            ComplexType where { (ComplexType.messageCount is_>= 0) and (ComplexType.messageCount is_< 250)} in coll must haveSize(N/2)
        }
        "apply ~" in {
            import java.util.regex.Pattern
            import Pattern._

            ComplexType.user.name is_~ Pattern.compile("user3$", CASE_INSENSITIVE) in coll must haveSize(1)
            ComplexType.user.name like "^User3$".r in coll must haveSize(1)
        }
    }
    "Query optional" should {
        val dbColl = mongo.getCollection(CollName)
        val coll = dbColl of OptModel
        val N = 10

        doBefore {
            dbColl.drop
            Helper.fillWith(coll, N) {i =>
                val c = new OptModel(i, if (i < 5) Some("d"+i) else None)
                if (i % 2 == 0) c.comment = Some("comment"+i)
                c
            }
        }
        doAfter  { dbColl.drop }

        "have correct size" in {
            coll must haveSize(N)
        }
        "getCount by shape" in {
            OptModel where {OptModel.description.exists} in coll must haveSize(5)
            OptModel where {OptModel.comment.exists} in coll must haveSize(5)
            OptModel where {OptModel.comment.notExists} in coll must haveSize(5)
        }
        "find by shape" in {
            val c = OptModel where {OptModel.comment is "comment2"} in coll
            c must haveSize(1)
            c.headOption must beSome[OptModel].which{x =>
                x.id == 2 && x.description == Some("d2")
            }
        }
    }
    "Query mixed collection" should {
        val dbColl = mongo.getCollection(CollName)
        val N = 10

        doFirst {
            dbColl.drop
            Helper.fillWith (dbColl of CaseUserShape, N) {x => CaseUser("User"+x)}
            Helper.fillWith (dbColl of ComplexType, N) {x => new ComplexType(CaseUser("User"+x), x*10)}
        }
        doLast  {
            dbColl.drop
        }

        "have correct total size" in {
            dbColl.getCount must be_==(N*2)
        }
        "getCount by shape" in {
            CaseUserShape where CaseUserShape.allFields.map{_.exists}.reduceLeft{_ and _} in (dbColl of CaseUserShape) must haveSize(N)
            ComplexType where ComplexType.allFields.map{_.exists}.reduceLeft{_ and _} in (dbColl of ComplexType) must haveSize(N)
        }
        "findOne by shape" in {
            dbColl.of(CaseUserShape).headOption must beSome[CaseUser].which{_ == CaseUser("User0")}
            dbColl.of(ComplexType).headOption must beSome[ComplexType].which{_.user == CaseUser("User0")}
        }
        "find by shape" in {
            CaseUserShape where {CaseUserShape.name is_< "User3"} in dbColl.of(CaseUserShape) must haveSize(3)
            ComplexType where {ComplexType.user.name is_< "User3"} in dbColl.of(ComplexType) must haveSize(3)
        }
    }
    "Query collection of ref" should {
        object RefModel extends RefModelShape(mongo, "users")

        val users = mongo.getCollection("users") of CaseUserShape
        val posts = mongo.getCollection("posts") of RefModel

        doBefore { users.drop; posts.drop }
        doAfter  { users.drop; posts.drop }

        "find by ref" in {
            var user: CaseUser = CaseUser(Const)
            val noOidUser = CaseUser("EmptyOID")

            users << user
            posts += new RefModel("text", user)

            user.mongoOID must beSome[ObjectId]
            noOidUser.mongoOID must beNone
            RefModel where {RefModel.user is_== user} in posts must haveSize(1)
            RefModel where {RefModel.user is_== noOidUser} in posts must beEmpty
            RefModel where {RefModel.user isNot user} in posts must beEmpty
            RefModel where {RefModel.user isNot noOidUser} in posts must haveSize(1)
            RefModel where {RefModel.user isIn List(user)} in posts must haveSize(1)
            RefModel where {RefModel.user isIn List(noOidUser)} in posts must beEmpty
            RefModel where {RefModel.user notIn List(user)} in posts must beEmpty
            RefModel where {RefModel.user notIn List(noOidUser)} in posts must haveSize(1)
        }
    }
    "Query collection with arrays" should {
        import ArrayOfInt._

        val N = 10
        val objs = mongo.getCollection("objs") of ArrayModel

        doBefore {
            objs.drop
            Helper.fillWith(objs, N) {x =>
                val o = new ArrayModel(x)
                o.messages = List.tabulate(x%2+1)(y => y+x)
                o
            }
        }
        doAfter  { objs.drop }

        "have correct total size" in {
            objs must haveSize(N)
        }
        "find by array contents" in {
            ArrayModel where {ArrayModel.messages is_== 2} in objs must haveSize(2)
            ArrayModel where {ArrayModel.messages hasAll List(5,6)} in objs must haveSize(1)
        }
        "find by array size" in {
            ArrayModel where {ArrayModel.messages hasSize 2} in objs must haveSize(5)
        }
    }
    "Query collection with maps" should {
        import MapOfScalar._

        val N = 10
        val objs = mongo.getCollection("objs") of MapModel

        doBefore {
            objs.drop
            Helper.fillWith(objs, N) {x =>
                val o = new MapModel(x)
                o.counts = Map[String,Int]( List.tabulate(x%2+1)(y => y+x) map {x => x.toString -> x} :_* )
                o
            }
        }
        doAfter  { objs.drop }

        "have correct total size" in {
            objs must haveSize(N)
        }
        "find by map contents" in {
            MapModel where {MapModel.counts("6").exists} in objs must haveSize(2)
            MapModel where {MapModel.counts("5").exists and MapModel.counts("6").exists} in objs must haveSize(1)
            MapModel where {MapModel.counts("5") is_== 5} in objs must haveSize(1)
        }
    }
}
