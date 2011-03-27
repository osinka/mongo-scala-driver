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

package com.osinka.mongodb.wrapper

import com.mongodb.{DBObject, BasicDBObject}

object DBO {
    def empty = new BasicDBObject

    def dotNotation(l: List[String]) = l.mkString(".")

    /**
     * Build DBObject from Map[String,Any]. The method descends, i.e. converts
     * Map values as well
     */
    def fromMap(m: Map[String,Any]): DBObject = {
        import com.mongodb.{BasicDBObjectBuilder, BasicDBList}

        def wrap(obj: Any): Option[Any] = obj match {
            case m: Map[_, _] =>
                // to avoid type erasure warning
                Some( fromMap(m.asInstanceOf[Map[String, Any]]) )
            case iterable: Iterable[_] =>
                val ret = new BasicDBList
                for {(v, i) <- iterable.toList.zipWithIndex
                     wrapped <- wrap(v)}
                    ret.put(i, wrapped)
                Some(ret)
//            case ref: Ref[_] =>
            case None => None
            case Some(v) => wrap(v)
            case _ => Some(obj)
        }

        def acc(dbo: BasicDBObjectBuilder, leaf: (String, Any)): BasicDBObjectBuilder =
            wrap(leaf._2).map{dbo.append(leaf._1, _)} getOrElse dbo

        (m foldLeft BasicDBObjectBuilder.start)(acc(_, _)).get
    }

    /**
     * Interpret DBObject as Array and return it as a Seq
     */
    def toArray(dbo: DBObject): Seq[Any] =
        Iterator from(0) map {_.toString} takeWhile {dbo.containsField} map{dbo.get} toList

    /**
     * Merge many DBObjects into one. The latter can override keys in the former
     * ones.
     */
    def merge(dbo: DBObject*) = {
        val result = empty
        dbo foreach result.putAll
        result
    }
}
