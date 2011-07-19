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
package com.osinka.mongodb.wrapper

/**
 * Helper methods to create update operations
 * 
 * @author Alexander Azarov <azarov@osinka.com>
 */
object MongoOp {
    def op[T](op: String)(field: String, x: T) = op -> Map(field -> x)

    lazy val inc = op[Any]("$inc") _
    lazy val set = op[Any]("$set") _
    lazy val unset = op[Any]("$unset") _
    lazy val push = op[Any]("$push") _
    lazy val pushAll = op[Any]("$pushAll") _
    lazy val pop = op[Any]("$pop") _
    lazy val pull = op[Any]("$pull") _
    lazy val pullAll = op[Any]("$pullAll") _
    lazy val addToSet = op[Any]("$addToSet") _
    lazy val addEachToSet = (field: String, x: Any) => "$addToSet" -> Map(field -> Map("$each" -> x))
}
