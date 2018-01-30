import java.sql
import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.{BZip2Codec, GzipCodec, SnappyCodec}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2018/1/18.
  */
object ConnectCompent {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    val sc = spark.sparkContext
    val arr = ArrayBuffer[Path]()
    val path = new Path("/tmp")
    val data = getName(path, arr)
    val res = data.filter(x => x.toString.contains("part"))
    val sql = "insert into res_test(num,count) values (?,?)"
    val p = "\\d+".r
    for (r <- res) {
      val ori = sc.textFile(r.toString)
      ori.foreach(x => {
        val list = p.findAllIn(x).toList
        val num = list(0)
        val count = list.last
        getConn(sql, num, count)
      })
    }
    sc.stop()
  }
  def method: Unit = {
    val gg = "gongogn"
    val cc = "suotao"
    if (gg.equals(cc)) {
        println("diaoshi")
      println("ni shi")
    }
  }
  def getConn(sql: String, par1: String, par2: String): Unit = {
    var coon: Connection = null
    var ps: PreparedStatement = null
    //    val sql = "insert into res_test(num,count) values (?,?)"
    try {
      coon = DriverManager.getConnection("jdbc:mysql://10.1.3.96:3306/datav"
        , "datav"
        , "datav")
      ps = coon.prepareStatement(sql)
      ps.setString(1, par1)
      ps.setString(2, par2)
      ps.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (coon != null) {
        coon.close()
      }
    }


  }

  def getName(path: Path, arr: ArrayBuffer[Path]): ArrayBuffer[Path] = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://10.1.3.171:8020")
    //    conf.set("mapred.remote.os", "Linux")
    val fileSystem = FileSystem.get(conf)
    val list = fileSystem.listStatus(path)
    for (res <- list) {
      if (res.isDirectory()) {
        //      println(res.getPath)
        getName(res.getPath, arr)
      } else {
        arr.append(res.getPath)
      }
    }
    arr
  }

  def test1(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
    val sc = spark.sparkContext
    val data = sc.textFile("hdfs://10.1.3.171:8020/tmp/test.txt/*")
    //    data.saveAsTextFile("D://666/")
    sc.stop()
  }

  def test(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf()
    val sc = new SparkContext("local", "test", conf)

    // day11.tsv
    // 1 2
    // 2 3
    // 3 1
    // 4 5
    // 5 6
    // 6 7
    val graph = GraphLoader.edgeListFile(sc, "edge.txt").cache()

    println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph ")
    graph.vertices.collect.foreach(println(_))
    // (4,1)
    // (6,1)
    // (2,1)
    // (1,1)
    // (3,1)
    // (7,1)
    // (5,1)

    println("\n\n~~~~~~~~~ Confirm Edges Internal of graph ")
    graph.edges.collect.foreach(println(_))
    // Edge(1,2,1)
    // Edge(2,3,1)
    // Edge(3,1,1)
    // Edge(4,5,1)
    // Edge(5,6,1)
    // Edge(6,7,1)
    // 1,2,3相连，4,5,6,7相连

    // 取连通图，连通图以图中最小Id作为label给图中顶点打属性
    val cc: Graph[Long, Int] = graph.connectedComponents

    println("\n\n~~~~~~~~~ Confirm Vertices Connected Components ")
    cc.vertices.collect.foreach(println(_))
    // (4,4)
    // (6,4)
    // (2,1)
    // (1,1)
    // (3,1)
    // (7,4)
    // (5,4)

    // 取出id为2的顶点的label
    val cc_label_of_vid_2: Long = cc.vertices.filter { case (id, label) => id == 2 }.first._2

    println("\n\n~~~~~~~~~ Confirm Connected Components Label of Vertex id 2")
    println(cc_label_of_vid_2)
    // 1

    // 取出相同类标的顶点
    val vertices_connected_with_vid_2: RDD[(Long, Long)] = cc.vertices.filter { case (id, label) => label == cc_label_of_vid_2 }

    println("\n\n~~~~~~~~~ Confirm vertices_connected_with_vid_2")
    vertices_connected_with_vid_2.collect.foreach(println(_))
    // (2,1)
    // (1,1)
    // (3,1)

    val vids_connected_with_vid_2: RDD[Long] = vertices_connected_with_vid_2.map(v => v._1)
    println("\n\n~~~~~~~~~ Confirm vids_connected_with_vid_2")
    vids_connected_with_vid_2.collect.foreach(println(_))
    // 2
    // 1
    // 3

    val vids_list: Array[Long] = vids_connected_with_vid_2.collect
    // 取出子图
    val graph_include_vid_2 = graph.subgraph(vpred = (vid, attr) => vids_list.contains(vid))

    println("\n\n~~~~~~~~~ Confirm graph_include_vid_2 ")
    graph_include_vid_2.vertices.collect.foreach(println(_))
    // (2,1)
    // (1,1)
    // (3,1)
    val sub = cc.subgraph(epred = triple => triple.srcAttr == cc_label_of_vid_2 && triple.dstAttr == cc_label_of_vid_2
      , vpred = (id, label) => label == cc_label_of_vid_2).triangleCount().vertices
    sub.filter(x => x._2 > 0).sortBy(x => x._1, false)
    val th = sub.filter(x => x._2 > 0).sortBy(x => x._1, false)
    //    th.saveAsTextFile("hdfs://10.1.3.171:8020/tmp/test.txt")
    th.foreach(println(_))
    sc.stop
  }

}
