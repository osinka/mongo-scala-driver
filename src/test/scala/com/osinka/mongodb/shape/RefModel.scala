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

class RefModel(val message: String, val user: CaseUser)

class RefModelShape(val db: DB, val usersCollName: String) extends ObjectShape[RefModel] { shape =>
    lazy val message = Field.scalar("message", _.message)
    lazy val user = Field.ref("user", CaseUserShape collection db.getCollection(usersCollName), _.user)
//
// same as
//
//    object user extends MongoScalar[CaseUser] with RefContent[CaseUser] with Functional[CaseUser] {
//        override val mongoFieldName = "user"
//        override lazy val coll: MongoCollection[CaseUser] = CaseUser collection db.getCollection(usersCollName)
//        override val rep = shape.Represented.by(_.user, None)
//    }

    override def factory: FactoryPF = {
        case message(m) ~ user(u) => new RefModel(m,u)
    }
}
