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

class OptModel(val id: Int, val description: Option[String]) {
    var comment: Option[String] = None
}

object OptModel extends ObjectShape[OptModel] {
    lazy val id = Field.scalar("id", _.id)

    // Hurray! Option[A] field!
    lazy val description = Field.optional("description", _.description)
    // OR much longer:
     
    object description3 extends OptionalField[String]("description", _.description, None)

    lazy val comment = Field.optional("comment", _.comment, (obj: OptModel, v: Option[String]) => obj.comment = v)

    override def storeFields(x: OptModel) = List(id, description, comment)

    val pf1: FactoryPF = {
      case id(i) ~ description(d) => new OptModel(i, Some(d))
    }
    val pf2: FactoryPF = {
      case id(i) => new OptModel(i, None)
    }

    override val factory: FactoryPF = pf1 orElse pf2
}
