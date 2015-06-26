/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink.example.eventpattern

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/**
 * A Flink data stream source that uses the [[EventsGenerator]] to produce a stream
 * of events.
 */
class EventsGeneratorSource(val addLatency: Boolean = false, val latencyCount: Int = 10000) extends RichParallelSourceFunction[(Event, Long)] {
  
  protected[this] var running = true

  protected[this] var count = 0

  override def run(sourceContext: SourceContext[(Event, Long)]): Unit = {

    val generator = new EventsGenerator()
    
    val range = Integer.MAX_VALUE / getRuntimeContext.getNumberOfParallelSubtasks()
    val min = range * getRuntimeContext.getIndexOfThisSubtask()
    val max = min + range


    while (running) {

      // Add valid timestamp for on record in every latencyCount
      var timeStamp = 0L
      if(addLatency && latencyCount <= count) {
        timeStamp = System.currentTimeMillis()
        count = 0
      }

      sourceContext.collect((generator.next(min, max), timeStamp))
      count += 1
    }
    
    // set running to false to stop the logger
    running = false
  }

  override def cancel(): Unit = {
    running = false
  }
}
