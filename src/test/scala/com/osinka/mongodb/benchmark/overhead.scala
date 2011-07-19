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
import org.specs.util.SimpleTimer

object overhead {
    val defaultArgs = List(10000, 1)
    val benchmarkSuites = SerializationOverhead :: ConstraintOverheadNoIndex :: ConstraintOverheadWithIndex :: Nil

    def main(args: Array[String]) {
        val List(collectionSize, repeat) = args.toList.map{_.toInt} ::: defaultArgs.drop(args.size).take(defaultArgs.size-args.size)

        for (suite <- benchmarkSuites)
            suite.runOn(collectionSize) {benchmark => report(benchmark.prefix, benchmark.runBenchmark(repeat)) }
    }

    def report(name: String, latencies: => List[Long]) {
        implicit def longToTimer(l: Long): SimpleTimer = { val t = new SimpleTimer; t.elapsed = l; t }

        // The first run is taken out, it's a warm up
        val sorted = latencies.tail.sortWith( (a,b) => a < b )

        val (total, count, min, max) = (
            (0L /: sorted)((x, y) => x + y),
            sorted.size,
            sorted.head,
            sorted.last
        )
        val avg = total / count

        System.err.println(name + ", " + count + " iterations:"
                           + " total=[" + total.time + "]"
                           + ", min=["  + min.time   + "]"
                           + ", avg=["  + avg.time   + "]"
                           + ", max=["  + max.time   + "]")
    }
}
