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

import com.mongodb.{DB,DBObject}
import com.osinka.mongodb._

object ArrayOfInt {
    class ArrayModel(val id: Int) {
        var messages: List[Int] = Nil

        override def toString = "ArrayModel("+id+", "+messages.mkString("[",",","]")+")"
    }

    object ArrayModel extends ObjectShape[ArrayModel] { shape =>
        lazy val id = Field.scalar("id", _.id)

        lazy val messages = Field.array("messages", _.messages, (x: ArrayModel, l: Seq[Int]) => x.messages = l.toList )
//
//   same as
//
//        object messages extends MongoArray[Int] with ScalarContent[Int] with ArrayFieldModifyOp[Int] {
//            override val mongoFieldName = "messages"
//            override val rep = Represented.by[Seq[Int]](_.messages, Some( (x: ArrayModel, l: Seq[Int]) => x.messages = l.toList ))
//        }

        override val factory: FactoryPF = {
            case id(i) => new ArrayModel(i)
        }
    }
}

object ArrayOfEmbedded {
    class ArrayModel(val id: Int, val users: List[CaseUser])

    object ArrayModel extends ObjectShape[ArrayModel] { shape =>
        lazy val id = Field.scalar("id", _.id)

        object users extends ArrayEmbeddedField[CaseUser]("users", _.users, None) with CaseUserIn[ArrayModel]
//
//   same as
//
//        object users extends MongoArray[CaseUser] with ArrayFieldModifyOp[CaseUser] with EmbeddedContent[CaseUser] with CaseUserIn[ArrayModel] {
//            override val mongoFieldName = "users"
//            override val rep = shape.Represented.by[Seq[CaseUser]]( _.users, Some( (x: ArrayModel, l: Seq[CaseUser]) => x.users = l.toList ))
//        }

        override val factory: FactoryPF = {
            case id(_id) ~ users(_users) => new ArrayModel(_id, _users.toList)
        }
    }
}

object ArrayOfRef {
    class ArrayModel(val id: Int) {
        var users: List[CaseUser] = Nil
    }

    class ArrayModelShape(val db: DB, val usersCollName: String) extends ObjectShape[ArrayModel] { shape =>
        lazy val id = Field.scalar("id", _.id)

        lazy val users = Field.arrayRef("users", CaseUser collection db.getCollection(usersCollName), _.users, (x: ArrayModel, l: Seq[CaseUser]) => x.users = l.toList )
//
//  same as
//
//        object users extends MongoArray[CaseUser] with RefContent[CaseUser] {
//            override val mongoFieldName = "users"
//            override lazy val coll: MongoCollection[CaseUser] = CaseUser collection db.getCollection(usersCollName)
//            override val rep = shape.Represented.by[Seq[CaseUser]]( _.users, Some( (x: ArrayModel, l: Seq[CaseUser]) => x.users = l.toList ))
//        }

        override val factory: FactoryPF = {
            case id(i) => new ArrayModel(i)
        }
    }
}
