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

import com.mongodb.{DBObject, DBCursor}

/**
 * Wrapper around DBCursor
 * 
 * @author Alexander Azarov <azarov@osinka.com>
 */
private[mongodb] class DBObjectIterator(val cursor: DBCursor) extends Iterator[DBObject] {
    override def hasNext: Boolean = cursor.hasNext
    override def next: DBObject = cursor.next
}