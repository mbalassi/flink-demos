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

import java.io.File
import java.util.{Random, Properties}

import _root_.kafka.consumer.ConsumerConfig
import com.dataartisans.flink.example.eventpattern.kafka.EventDeSerializer
import com.dataartisans.flink.util.latency.LatencyTester
import com.dataartisans.flink.util.throughput.PerformanceCounter

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.checkpoint.Checkpointed
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * Demo streaming program that receives (or generates) a stream of events and evaluates
 * a state machine (per originating IP address) to validate that the events follow
 * the state machine's rules.
 */
object StreamingDemo {

  def main(args: Array[String]): Unit = {

    val checkpointInterval = args(0).toInt
//    val intervalLength = args(0).toInt

    // create the environment to create streams and configure execution
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(4)
    
    // enables checkpointing if interval > 0
    if (checkpointInterval > 0){
      env.enableCheckpointing(checkpointInterval)
    }

    // create a data stream from the generator
    val stream = env.addSource(new EventsGeneratorSource(true))
    
    stream
      // partition on the address to make sure equal addresses
      // end up in the same state machine flatMap function
      .partitionByHash("sourceAddress")
      
      // the function that evaluates the state machine over the sequence of events
//      .flatMap(new StateMachineMapperLatency(checkpointInterval))
      .flatMap(new StateMachineMapperThroughput(checkpointInterval))

      // unused, just closes the topology
      .addSink(new SinkFunction[(Alert, Long)] {
      override def invoke(in: (Alert, Long)): Unit = {
      }
    })
    
    // trigger program execution
    env.execute()
  }
}



/**
 * The function that maintains the per-IP-address state machines and verifies that the
 * events are consistent with the current state of the state machine. If the event is not
 * consistent with the current state, the function produces an alert.
 */
class StateMachineMapperThroughput(checkpointInterval : Int, counterInterval : Long = 100000) extends RichFlatMapFunction[(Event, Long), (Alert, Long)] with Checkpointed[mutable.HashMap[Int, State]] {
  
  private[this] val states = new mutable.HashMap[Int, State]()

  @transient
  var pCounter: PerformanceCounter = null
  // batches up counts in the operator so reducing system time call frequencies
  var counter = 0L

  @throws(classOf[Exception])
  override def open(parameters: Configuration) {
    pCounter = new PerformanceCounter("pc", 1000, 100000, 30000,  "/home/mbalassi/flink-state-perf-" +
      checkpointInterval + "-" + getRuntimeContext.getIndexOfThisSubtask + ".csv")
  }

  override def flatMap(tuple: (Event, Long), out: Collector[(Alert, Long)]): Unit = {

    if (counter >= counterInterval){
      pCounter.count(counter)
      counter = 0L
    }

    val t = tuple._1

    // get and remove the current state
    val state = states.remove(t.sourceAddress).getOrElse(InitialState)

    val nextState = state.transition(t.event)
    if (nextState == InvalidTransition) {
      // Output is unused for cluster measurements
      //out.collect(Alert(t.sourceAddress, state, t.event))
    } 
    else if (!nextState.terminal) {
      states.put(t.sourceAddress, nextState)
    }

    counter += 1
  }

  /**
   * Draws a snapshot of the function's state.
   * 
   * @param checkpointId The ID of the checkpoint.
   * @param timestamp The timestamp when the checkpoint was instantiated.
   * @return The state to be snapshotted, here the hash map of state machines.
   */
  override def snapshotState(checkpointId: Long, timestamp: Long): mutable.HashMap[Int, State] = {
    states
  }

  /**
   * Restores the state.
   * 
   * @param state The state to be restored.
   */
  override def restoreState(state: mutable.HashMap[Int, State]): Unit = {
    states ++= state
  }
}

/**
 * The function that maintains the per-IP-address state machines and verifies that the
 * events are consistent with the current state of the state machine. If the event is not
 * consistent with the current state, the function produces an alert.
 */
class StateMachineMapperLatency(intervalLength : Int) extends RichFlatMapFunction[(Event, Long), (Alert, Long)] with Checkpointed[mutable.HashMap[Int, State]] {

  private[this] val states = new mutable.HashMap[Int, State]()

  @transient
  var latencyTester: LatencyTester = null

  @throws(classOf[Exception])
  override def open(parameters: Configuration) {
    latencyTester = new LatencyTester(intervalLength, getFileName)
  }

  override def flatMap(tuple: (Event, Long), out: Collector[(Alert, Long)]): Unit = {

    val t = tuple._1

    // get and remove the current state
    val state = states.remove(t.sourceAddress).getOrElse(InitialState)

    val nextState = state.transition(t.event)
    if (nextState == InvalidTransition) {
      // Output is unused for cluster measurements
      //out.collect(Alert(t.sourceAddress, state, t.event))
    }
    else if (!nextState.terminal) {
      states.put(t.sourceAddress, nextState)
    }

    // when getting a valid timestamp add it to the latency tester
    if (tuple._2 != 0) {
      latencyTester.add(tuple._2, System.currentTimeMillis())
    }
  }

  /**
   * Draws a snapshot of the function's state.
   *
   * @param checkpointId The ID of the checkpoint.
   * @param timestamp The timestamp when the checkpoint was instantiated.
   * @return The state to be snapshotted, here the hash map of state machines.
   */
  override def snapshotState(checkpointId: Long, timestamp: Long): mutable.HashMap[Int, State] = {
    states
  }

  /**
   * Restores the state.
   *
   * @param state The state to be restored.
   */
  override def restoreState(state: mutable.HashMap[Int, State]): Unit = {
    states ++= state
  }

  private def getFileName: String = {
    val rnd: Random = new Random
    var fileName: String = null
    var histFile: File = null
    do {
      fileName = "/home/mbalassi/histogramPart-" + intervalLength + "-" + String.valueOf(rnd.nextInt(10000000)) + ".csv"
      histFile = new File(fileName)
    } while (histFile.exists)
    return fileName
  }

}