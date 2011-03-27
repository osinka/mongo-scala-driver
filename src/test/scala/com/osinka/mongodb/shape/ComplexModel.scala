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
import com.osinka.mongodb._

class ComplexType(val user: CaseUser, val messageCount: Int) extends MongoObject {
    override def toString = "ComplexType (" + user + ", " + messageCount + ")"
}

object ComplexType extends MongoObjectShape[ComplexType] with FunctionalShape[ComplexType] {
    object user extends EmbeddedField[CaseUser]("user", _.user, None) with CaseUserIn[ComplexType]
    object messageCount extends ScalarField[Int]("msgs", _.messageCount, None)

    override val factory: FactoryPF = {
        case user(u) ~ messageCount(x) => new ComplexType(u, x)
    }
}
