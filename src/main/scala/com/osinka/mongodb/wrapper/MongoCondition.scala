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

import java.util.regex.Pattern
import scala.util.matching.Regex

import com.mongodb.QueryOperators._

/**
 * Helper methods to create conditions on fields to build queries
 * 
 * @author Alexander Azarov <azarov@osinka.ru>
 */
object MongoCondition {
    def cond[T](field: String, x: T) = field -> x

    def op[T](op: String)(field: String, x: T) = cond(field, Map(op -> x))

    def eqTest[T](field: String, x: T) = cond(field, x)
    lazy val neTest = op[Any](NE) _

    lazy val lt = op[Any](LT) _
    lazy val le = op[Any](LTE) _
    lazy val gt = op[Any](GT) _
    lazy val ge = op[Any](GTE) _
    lazy val in = op[Any](IN) _
    lazy val nin = op[Any](NIN) _
    lazy val all = op[Any](ALL) _
//    def mod
    lazy val size = op[Any](SIZE) _
    lazy val elemMatch = op[Any]("$elemMatch") _
    def exists(field: String, b: Boolean) = op(EXISTS)(field, b)

    def regex(field: String, x: Regex): (String, Pattern) = regex(field, x.pattern)
    def regex(field: String, x: Pattern): (String, Pattern) = eqTest(field, x)
}
