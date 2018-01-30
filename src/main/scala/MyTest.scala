import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by Administrator on 2017/12/25.
  */
object MyTest1 {
  def main(args: Array[String]): Unit = {
    val a = Array(1,2,3,4,5,6,7,8)
    val map = Map(1->"a","2"->"b",3->"c")
    println(map.get(2).isEmpty)
  }

  def test7(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    val sc = spark.sparkContext
    //构造graph
    val users:RDD[(VertexId,(String,String))] = sc.parallelize(Array(
      (3L,("rxin","student")),(7L,("jgonzal","postdoc")),
      (5L,("frank","prof")),(2L,("istcal","prof"))
    ))
    val relationships:RDD[Edge[String]] = sc.parallelize(Array(
      Edge(3L,7L,"collbol"),Edge(5L,3L,"advior"),
      Edge(2L,5L,"colleague"),Edge(5L,7L,"pi")
    ))
    val defaultUser = ("John","Missing")
    val graph = Graph(users,relationships,defaultUser)
    //点RDD、边RDD的过滤
    val fcount1 = graph.vertices.filter{case (id,(name,pos)) => pos == "postdoc"}.count()
    println("post user count:"+fcount1)
    val fcount2 = graph.edges.filter(edge => edge.srcId>edge.dstId).count()
    println("edge.srcId > edge.dstId count :"+fcount2)
    val fcount3 = graph.edges.filter{case Edge(src,dst,prop)=>src >dst}.count()
    println("edge.srcId > edge.dstId count :"+fcount3)
    //Triplets (三元组) 包含源点、源点属性、目标点、目标点属性、边属性
    val triplets:RDD[String] = graph.triplets.map(triplets => triplets.srcId+"-"+triplets.srcAttr._1+"-"+
      triplets.dstId+"-"+triplets.dstAttr._1+"-"+triplets.attr)
    triplets.foreach(println(_))
    //度数
    val degrees = graph.degrees
    degrees.foreach(println(_))
    val inDregrees = graph.inDegrees
    inDregrees.foreach(println(_))
    val outDregrees = graph.outDegrees
    outDregrees.foreach(println(_))
    println("------------------------")
    val subGraph = graph.subgraph(vpred = (id,attr)=>attr._2!="Missing")
    subGraph.vertices.foreach(println(_))
    println("****************")
    subGraph.triplets.map(triplet=>triplet.srcAttr._1+" is the "+triplet.attr + " of "+ triplet.dstAttr._1).collect().foreach(println)
    //Map操作
    val newGraphVertexed = graph.vertices.map{case (id,attr)=>(id,(attr._1+"-1",attr._2+"-2"))}
    val newGraph1 = Graph(newGraphVertexed,graph.edges)
    val newGraph2 = graph.mapVertices((id,attr)=>(id,(attr._1+"-1",attr._2+"-2")))
  //**********************************未完待续*******************************************************
    //构造新图 顶点属性是出度
    val inputGraph:Graph[Int,String]=graph.outerJoinVertices(graph.outDegrees)((vid,_,degOpt)=>degOpt.getOrElse(0))
    //根据顶点属性为出度的图构造一个新图 依据PageRank算法初始化边与点
    val outGraph:Graph[Double,Double] = inputGraph.mapTriplets(triplet => 1.0/triplet.srcAttr).mapVertices((id,_)=>1.0)
    //图的反向操作 所有边的方向相反 其他不变
    var rGraph = graph.reverse
    //mask
    val ccGraph = graph.connectedComponents()
    println("ccGraph")
    ccGraph.vertices.foreach(println(_))
    val validGraph = graph.subgraph(vpred = (id,attr)=> attr._2 != "Missing" )
    println("validGraph")
    validGraph.vertices.foreach(println(_))
    val validCCGraph = ccGraph.mask(validGraph)
    println("mask")
    validCCGraph.vertices.foreach(println)
    //join操作，原图外连出度点构造一个新图，出度为顶点属性
    val degreeGraph2 = graph.outerJoinVertices(outDregrees){(id,attr,outDegreeOpt)=>
      outDegreeOpt match {
        case Some(outDeg) => outDeg
        case None => 0
      }
    }
    sc.stop()
  }


  def test6(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.textFile("D://123.txt")
    val graph = GraphLoader.edgeListFile(sc,"D://123.txt")
    graph.degrees.foreach(println(_))
    sc.stop()
  }
  def test5(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    //加载图
    val graph = GraphLoader.edgeListFile(sc,"G://scala/spark-2.2.0-bin-hadoop2.7/data/graphx/followers.txt")
    //定义max函数
    def max(a:(VertexId,Int),b:(VertexId,Int)):(VertexId,Int)  ={
      if (a._2>b._2) a else b
    }
    //入度 被指向的个数 最多的那个顶点
    val inDMax = graph.inDegrees.reduce(max)
    println(inDMax)
    //出度 该顶点指向其他顶点的个数 最多的
    println("---------------")
    val outDMax = graph.outDegrees.reduce(max)
    println(outDMax)
    //度数 指向和被指向的总个数 最多的
    println("---------------")
    val DMax = graph.degrees.reduce(max)
    println(DMax)
    sc.stop()
  }
  def test4(): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    //加载图
    val graph = GraphLoader.edgeListFile(sc,"G://scala/spark-2.2.0-bin-hadoop2.7/data/graphx/followers.txt")
    //定义max函数
    def max(a:(VertexId,Int),b:(VertexId,Int)):(VertexId,Int) = {
      if (a._2>b._2) a else b
    }
    //入度 被指向的个数
    val inD = graph.inDegrees
    inD.foreach(println(_))
    //出度 该顶点指向其他顶点的个数
    println("---------------")
    val outD = graph.outDegrees
    outD.foreach(println(_))
    //度数 指向和被指向的总个数
    println("---------------")
    val D = graph.degrees
    D.foreach(println(_))
    //
    sc.stop()
  }
  def test3(args: Array[String]): Unit = {
    val reg = "^((1[3,5,8][0-9])|(14[5,7])|(17[0,6,7,8])|(19[7]))\\d{8}$".r
    val data = "12462457848"
    val list = reg findAllIn (data) toList;
    println(list)
  }

  def test2(args: Array[String]): Unit = {
    val now = 2784
    val s = new Random()
    val ss = s.nextInt(2)
    val hao1 = -1500 + (-800 * ss)
    val hao10 = (-2628) + (-2000) + 9600
    val hao20 = -2000
    val res = now + hao1 + hao10 + hao20
    println(res)
    println(ss)
    println(Int.MaxValue)

    val str = "new"
    val oo = str match {
      case data if str.contains("n") => "con"
      case da if da.startsWith("n") => "nnnnnn"
      case "spark" => println("spark")
    }
    println(oo)
  }

  def test(args: Array[String]): Unit = {
    val str = "根据运营商详单数据，165天内关机2天，连续三天以上关机0次"
    val p = "\\d+".r
    val l = p.findAllIn(str).toList
    println(l.last)
  }
}
