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
package com.osinka.mongodb.benchmark

import scala.testing.Benchmark

/**
 * @author Alexander Azarov <azarov@osinka.com>
 */
abstract class BenchmarkSuite(val name: String) {
    def benchmarks: List[Benchmark]
    
    def setUp(collSize: Int): Unit
    def tearDown: Unit

    def runOn[R](collSize: Int)(f: (Benchmark => R)): List[R] =
        try {
            System.err.println(name)
            setUp(collSize)
            benchmarks map f
        } finally {
            tearDown
        }
}
