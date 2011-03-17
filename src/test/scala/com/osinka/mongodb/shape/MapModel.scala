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

object MapOfScalar {
    class MapModel(val id: Int) {
        var counts: Map[String, Int] = Map.empty

        override def toString = "ArrayModel("+id+", "+counts.mkString("[",",","]")+")"
    }

    object MapModel extends ObjectShape[MapModel] { shape =>
        lazy val id = Field.scalar("id", _.id)

        lazy val counts = Field.map("counts", _.counts, (x: MapModel, l: Map[String,Int]) => x.counts = l)

        lazy val * = List(id, counts)
        override def factory(dbo: Option[DBObject]) = for {_id <- id from dbo} yield new MapModel(_id)
    }
}

object MapOfEmbedded {
    class MapModel(val id: Int, val users: Map[String,CaseUser]) {
        override def toString = "MapModel("+id+","+users+")"
    }

    object MapModel extends ObjectShape[MapModel] { shape =>
        lazy val id = Field.scalar("id", _.id)

        object users extends MapEmbeddedField[CaseUser]("users", _.users, None) with CaseUserIn[MapModel] { field =>

            /**
             * apply method makes it possible to build queries where condition is set on map value, e.g.
             * MapModel.users("strID").exists
             * or
             * MapModel.users("strID").name is_== "John"
             *
             * if you do not need such queries, there is no need in "apply" here
             */
            def apply(key: String) = new shape.EmbeddedField[CaseUser](key, _.users(key), None) with CaseUserIn[MapModel] {
                override def mongoFieldPath = field.mongoFieldPath ::: super.mongoFieldPath
            }
        }

        lazy val * = List(id, users)
        override def factory(dbo: Option[DBObject]) = for {_id <- id from dbo; _users <- users from dbo} yield new MapModel(_id, _users)
    }
}