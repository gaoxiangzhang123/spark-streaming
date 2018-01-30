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
import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy, VertexId}
import org.apache.spark.util.AccumulatorV2
// $example off$
import org.apache.spark.sql.SparkSession

/**
  * A vertex is part of a triangle when it has two adjacent vertices with an edge between them.
  * GraphX implements a triangle counting algorithm in the [`TriangleCount` object][TriangleCount]
  * that determines the number of triangles passing through each vertex,
  * providing a measure of clustering.
  * We compute the triangle count of the social network dataset.
  *
  * Note that `TriangleCount` requires the edges to be in canonical orientation (`srcId < dstId`)
  * and the graph to be partitioned using [`Graph.partitionBy`][Graph.partitionBy].
  *
  * Run with
  * {{{
  * bin/run-example graphx.TriangleCountingExample
  * }}}
  */
object TriangleCountingExample {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    // $example on$
    // Load the edges in canonical order and partition the graph for triangle count
    //按规范顺序加载边缘，并将图形划分为三角形计数。
    /*val graph = GraphLoader.edgeListFile(sc, "G://scala/spark-2.2.0-bin-hadoop2.7/data/graphx/followers.txt", true)
//      .partitionBy(PartitionStrategy.RandomVertexCut)
// Find the triangle count for each vertex找到每个顶点的三角形计数
val triCounts = graph.triangleCount().vertices
val ss = triCounts.collect()
ss.foreach(println(_))*/
    /** ******************************************************/
    // Load the graph as in the PageRank example
    val graph = GraphLoader.edgeListFile(sc, "G://scala/spark-2.2.0-bin-hadoop2.7/data/graphx/followers.txt")
    // Find the connected components
    val triCount = graph.triangleCount().subgraph(vpred = (id,num)=>num>0)
    val tcc = triCount.connectedComponents()
    val accc = new SetAccumulator()
    sc.register(accc)
    //三角形关系顶点
//    tcc.vertices.foreach(println(_))
    //
    tcc.triplets.foreach(x=>{
      println("srcAttr"+x.srcAttr)
      println("srcId"+ x.srcId)
      println("dstAttr"+x.dstAttr)
      println("dstId"+x.dstId)
      println("-----------")
    })
    val count = tcc.vertices.map(x=>{
      accc.add(x._2)
    }).count()
    val cid = accc.value
    val iter = cid.iterator()//nadaozhigezhi
    while (iter.hasNext) {
        val next = iter.next()
      val gra = tcc.subgraph(epred = triplet =>triplet.srcAttr==next&&triplet.dstAttr==next,vpred = (vid,data)=>data==next).triangleCount().vertices
      val res = gra.filter(x=>x._2>0).sortBy(x=>x._2,false)
      println(next)
//    res.foreach(println(_))
    }
//    tcc.subgraph()
//    println(count.count())
//    cc.vertices.foreach(println(_))
    // Join the connected components with the usernames


    /** ******************************************************/
    //triCounts
    /*(4,0)
   (1,0)
   (6,1)
   (3,1)
   (7,1)
   (2,0)*/

    /* //打印边信息
     graph.edges.foreach(println(_))
     // Join the triangle counts with the usernames使用用户名加入三角形计数。
     val users = sc.textFile("G://scala/spark-2.2.0-bin-hadoop2.7/data/graphx/users.txt").map { line =>
       val fields = line.split(",")
       (fields(0).toLong, fields(1) + "," + fields(2))
     }
     /* users
     1,BarackObama
     2,ladygaga
     3,jeresig
     4,justinbieber
     6,matei_zaharia
     7,odersky
     8,anonsys */
     val triCountByUsername = users.join(triCounts).map { case (id, (username, tc)) =>
       (username, tc)
     }
     // Print the result
     println(triCountByUsername.collect().mkString("\n"))
     // $example off$*/
    sc.stop()

  }
}

// scalastyle:on println
