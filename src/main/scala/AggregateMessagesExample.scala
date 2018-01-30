/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
// $example on$
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexRDD}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.storage.StorageLevel
// $example off$
import org.apache.spark.sql.SparkSession

/**
  * An example use the [`aggregateMessages`][Graph.aggregateMessages] operator to
  * compute the average age of the more senior followers of each user
  * Run with
  * {{{
  * bin/run-example graphx.AggregateMessagesExample
  * }}}
  */
object GraphExample {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    test1(sc)

    spark.stop()
  }

  def test2: Unit = {
//    val graph = Graph(Vector[String],Edge[String,String]())


  }

  def test1(sc: SparkContext): Unit = {
    // $example on$
    // Create a graph with "age" as the vertex property.
    // Here we use a random graph for simplicity.
    val graph: Graph[Double, Int] =
    GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices((id, _) => id.toDouble)

//    val s = GraphLoader.edgeListFile(sc,"",false,3,StorageLevel.MEMORY_AND_DISK)
   /* s.edges.map(x=>{
      val s = x.attr
      x.dstId
      x.srcId
    })*/

    // Compute the number of older followers and their total age
    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      triplet => { // Map Function
        if (triplet.srcAttr > triplet.dstAttr) {
          // Send message to destination vertex containing counter and age
          triplet.sendToDst(1, triplet.srcAttr)
        }
      },
      // Add counter and age
      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )
    val a = Array(
      Edge(1,2,3),Edge(2,3,4)
    )
    // Divide total age by number of older followers to get average age of older followers
    val avgAgeOfOlderFollowers: VertexRDD[Double] =
      olderFollowers.mapValues((id, value) =>
        value match {
          case (count, totalAge) => totalAge / count
        })
    // Display the results
    avgAgeOfOlderFollowers.collect.foreach(println(_))
    // $example off$

  }
}

// scalastyle:on println
