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
import com.mongodb._

import com.osinka.mongodb._
import Config._

object updateSpec extends Specification("Update") {
    val CollName = "test"
    val Const = "John Doe"

    val mongo = new Mongo(Host, Port).getDB(Database)

    doAfter { mongo.dropDatabase }

    "Update scalar" should {
        val dbColl = mongo.getCollection(CollName)
        val coll = dbColl of ComplexType
        val N = 50

        doBefore {
            dbColl.drop
            Helper.fillWith(coll, N) {x => new ComplexType(CaseUser("User"+x), x*10)}
        }
        doAfter {
            dbColl.drop
        }

        "$set" in {
            (coll(ComplexType.user.name is_== "User1") = ComplexType.messageCount set 1) must beTrue
            (ComplexType.user.name is_== "User1" in coll).headOption must beSome[ComplexType].which{_.messageCount == 1}
        }
        "$set in embedded" in {
            (coll(ComplexType.user.name is_== "User1") = ComplexType.user.name set "User2") must beTrue
            (ComplexType.user.name is_== "User1" in coll) must beEmpty
            (ComplexType.user.name is_== "User2" in coll) must haveSize(2)
        }
        "$set embedded" in {
            (coll(ComplexType.messageCount is_== 10) = ComplexType.user set CaseUser("User2")) must beTrue
            (ComplexType.user.name is_== "User1" in coll) must beEmpty
            (ComplexType.user.name is_== "User2" in coll) must haveSize(2)
        }
        "$unset" in {
            coll(ComplexType.user.name is_== "User1") = ComplexType.messageCount.unset
            (ComplexType.messageCount.exists in coll) must haveSize(N-1)
        }
        "$inc" in {
            (coll(ComplexType.user.name is_== "User1") = (ComplexType.messageCount inc 10)) must beTrue
//            System.err.println("==>" + (ComplexType.user.name is_== "User1" in coll).mkString(","))
            (ComplexType.messageCount is_== 10 in coll) must beEmpty
            (ComplexType.messageCount is_== 20 in coll) must haveSize(2)
        }
        "do two modifiers for all" in {
            (coll(ComplexType.any) = (ComplexType.messageCount inc -100) and (ComplexType.user.name set "User2") ) must beTrue
            coll must haveSize(N)
            (ComplexType.user.name is_== "User1" in coll) must beEmpty
            (ComplexType.user.name is_== "User2" in coll) must haveSize(N)
            (ComplexType.messageCount is_< 0 in coll) must haveSize(10)
        }
        "set two fields at once" in {
            (coll(ComplexType.any) = (ComplexType.messageCount set 200) and (ComplexType.user.name set "User200") ) must beTrue
            val r = ComplexType.messageCount is_== 200 in coll
            r must haveSize(N)

            val head = r.headOption
            head must beSome[ComplexType]
            head.get.messageCount must be_==(200)
            head.get.user.name must be_==("User200")
        }
    }
    "Update array of scalars" should {
        import ArrayOfInt._

        val N = 10
        val objs = mongo.getCollection(CollName) of ArrayModel

        doBefore {
            Helper.fillWith(objs, N) {x =>
                val o = new ArrayModel(x)
                o.messages = List.tabulate(x%2+1)(y => y+x)
                o
            }
        }
        doAfter  { objs.drop }

        "$set Seq[T]" in {
            objs( ArrayModel.id is_== 0 ) = ArrayModel.messages set List(10)
            (ArrayModel.id is_== 0 in objs).headOption must beSome[ArrayModel].which{_.messages == List(10)}
        }
        "$unset" in {
            objs( ArrayModel.id is_== 0 ) = ArrayModel.messages.unset
            (ArrayModel.id is_== 0 in objs).headOption must beSome[ArrayModel].which{_.messages.isEmpty}
        }
        "$push" in {
            objs map {_.messages.size} reduceLeft {_ max _} must be_==(2)
            (objs(ArrayModel.any) = ArrayModel.messages push 500) must beTrue
            objs map {_.messages.size} reduceLeft {_ max _} must be_==(3)
            (ArrayModel.messages hasSize 3 in objs) must haveSize(5)
        }
        "$pushAll" in {
            objs map {_.messages.size} reduceLeft {_ max _} must be_==(2)
            (objs(ArrayModel.any) = ArrayModel.messages pushAll List(50,60)) must beTrue
            objs map {_.messages.size} reduceLeft {_ max _} must be_==(4)
            (ArrayModel.messages hasSize 3 in objs) must haveSize(5)
        }
        "$popHead" in {
            val q = ArrayModel.id is_== 1
            objs.update(q, ArrayModel.messages.popHead) must beTrue
            (q in objs).headOption must beSome[ArrayModel].which{_.messages == List(2)}
        }
        "$popTail" in {
            val q = ArrayModel.id is_== 1
            objs.update(q, ArrayModel.messages.popTail) must beTrue
            (q in objs).headOption must beSome[ArrayModel].which{_.messages == List(1)}
        }
        "$pull" in {
            (objs(ArrayModel.id in List(5,6)) = ArrayModel.messages pull 6) must beTrue
            (ArrayModel.id is_== 6 in objs).headOption must beSome[ArrayModel].which{_.messages == Nil}
        }
        "$pullAll" in {
            (objs(ArrayModel.id in List(5,6)) = ArrayModel.messages pullAll List(5,6)) must beTrue
            (ArrayModel.id is_== 5 in objs).headOption must beSome[ArrayModel].which{_.messages == Nil}
        }
        "$addToSet" in {
            (ArrayModel.id is_== 5 in objs).headOption must beSome[ArrayModel].which{_.messages == List(5, 6)}
            objs(ArrayModel.id in List(5, 6)) = ArrayModel.messages addToSet 5
            (ArrayModel.id is_== 5 in objs).headOption must beSome[ArrayModel].which{_.messages == List(5, 6)}
            objs(ArrayModel.id in List(5, 6)) = ArrayModel.messages addToSet 7
            (ArrayModel.id is_== 5 in objs).headOption must beSome[ArrayModel].which{_.messages == List(5, 6, 7)}
        }
        "$addToSet with $each" in {
            (ArrayModel.id is_== 5 in objs).headOption must beSome[ArrayModel].which{_.messages == List(5, 6)}
            objs(ArrayModel.id in List(5, 6)) = ArrayModel.messages addToSet List(6, 7)
            (ArrayModel.id is_== 5 in objs).headOption must beSome[ArrayModel].which{_.messages == List(5, 6, 7)}
        }
    }
}
