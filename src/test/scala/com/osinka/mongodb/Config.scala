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

package com.osinka.mongodb

import java.util.Properties

object Config {
  private val fileName = "database.properties"

  private lazy val properties: List[Properties] =
    for {classLoader <- getClass.getClassLoader :: Thread.currentThread.getContextClassLoader :: Nil
         stream <- Option(classLoader.getResourceAsStream(fileName))}
    yield
      try {
        val props = new Properties
        props.load(stream)
        props
      } finally {
        stream.close
      }

  private def property[T](k: String)(implicit conv: String => T): Option[T] =
    properties flatMap{p => Option(p getProperty k)} map {conv} headOption
    
  implicit val stringToInt = (s: String) => s.toInt

  lazy val Host = property[String]("mongoHost") getOrElse "localhost"
  lazy val Port = property[Int]("mongoPort") getOrElse 27017
  lazy val Database = property[String]("mongoDB") getOrElse "test"
}