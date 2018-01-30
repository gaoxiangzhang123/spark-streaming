import java.io.{File, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.regex.Pattern

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by Administrator on 2018/1/3.
  */
object SparkGraphx {
  def main(args: Array[String]): Unit = {
    /*val file  = new File("D://result-1000")
    getFileName(file).foreach(println(_))*/
//        test3()
//    D:\\datatest\\origintest\\666.txt
   /* val s = Source.fromFile("D:\\datatest\\origintest\\666.txt")
    if (s.getLines()!=null) {
        val w = s.getLines()
      w.foreach(println(_))
    }*/
    method1
    val sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val ti = "2018-3-4"
  }
  def method1(): Unit = {
    val file = new File("D://myTest")
    println(file.getAbsolutePath)
    val fileNameArr = getFileName(file)
    for (arr <- fileNameArr) {
      val name = arr.toString.replace("\\", "\\\\")
      println(name)
      val res = Source.fromFile(name)
      if (res.getLines()!=null) {
        val s = res.getLines()
        for (data <- s) {
          println(data)
        }
        res.close()
      }
      Thread.sleep(5000)
      arr.delete()
    }
  }
  def method: Unit = {
    /*测试Unit*/
    val name = ""

    val ma = ""
    if (name==ma) {
        println("相同")
    } else {
      println("不同")
    }
  }

  def getFileName(file: File): Array[File] = {
    val fileNameArr = file.listFiles().filter(!_.isDirectory)
      .filter(x => x.toString.endsWith(".txt"))
    fileNameArr ++ file.listFiles().filter(_.isDirectory).flatMap(getFileName)
  }
  def test3(): Unit = {
    //    test1()
    val file = new File("D://datatest/origintest")
    val fileName = getFileName(file)
    fileName.foreach(println(_))
    val write = new FileWriter("D://rrrr.txt",true)
    for (elem <- fileName) {
      val name = elem.toString.replace("\\","\\\\")
      println(name)
      val data = Source.fromFile(name)
      if (data.getLines()!= null) {
        val res = data.getLines()
        res.foreach(x=>{
          write.write(x)
          write.flush()
          Thread.sleep(2000)
        })
      }
    }
    write.close()
  }

  def test1(): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("Graphx").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //创建Hbase连接
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.zookeeper.quorum", "10.1.3.170,10.1.3.171,10.1.3.172")
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val userTable = TableName.valueOf("TSP_CALLS")
    val table = conn.getTable(userTable)
    val s = new Scan()
    s.addColumn("basic".getBytes, "cell_phone".getBytes)
    s.addColumn("basic".getBytes, "other_cell_phone".getBytes())
    s.addColumn("basic".getBytes, "init_type".getBytes())
    val scanner = table.getScanner(s)
    val reg = "^((1[3,5,8][0-9])|(14[5,7])|(17[0,6,7,8])|(19[7]))\\d{8}$".r
    try {
      val arr = ArrayBuffer[Tuple2[VertexId, VertexId]]()
      for (r <- scanner) {
        val cellPhone = Bytes.toString(r.getValue("basic".getBytes, "cell_phone".getBytes))
        val otherCellPhone = Bytes.toString(r.getValue("basic".getBytes, "other_cell_phone".getBytes))
        val initType = Bytes.toString(r.getValue("basic".getBytes, "init_type".getBytes))
        val num = reg.findAllIn(otherCellPhone).toList
        if (num.size!=0) {
          if (initType.contains("被叫")) {
            val arr1 = new Tuple2[VertexId, VertexId](num(0).toLong, cellPhone.toLong)
            arr.append(arr1)
          };
          if (initType.contains("主叫")) {
            val arr1 = new Tuple2[VertexId, VertexId](cellPhone.toLong, num(0).toLong)
            arr.append(arr1)
          }

        }


      }

      val rdd = sc.makeRDD(arr.distinct)
      val graph = Graph.fromEdgeTuples(rdd, 1).partitionBy(PartitionStrategy.RandomVertexCut)
      graph.triangleCount().vertices.foreach(println(_))
      //      graph.vertices.collect().foreach(println(_))
      //      graph.pageRank(0.01).vertices.collect().foreach(println(_))
      /*graph.edges.collect().foreach {
        case (Edge(src, dst, prop)) => println((src, dst, prop))
      }*/
    } finally {
      //确保scanner关闭
      scanner.close()
    }


    /*   val vertexArray = Array(
         (1L,"123456789"),
         (2L,"987654321")
       )
       val edgeArray = Array(
         Edge(1L,2L,1),
         Edge(2L,1L,2)
       )

       val vertexRDD:RDD[(Long,String)] = sc.parallelize(vertexArray)
       val edgeRdd:RDD[(Edge[Int])] = sc.parallelize(edgeArray)
       val graph:Graph[String,Int] = Graph(vertexRDD,edgeRdd)

       graph.vertices.collect().foreach{
         case (id,num) => println(s"${num}")
       }*/
    conn.close()
    table.close()
    sc.stop()
  }


  //
  def test2(): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    var tableName = "TSP_CALLS"
    val sparkConf = new SparkConf().setAppName("graphx").setMaster("local")
    val sc: SparkContext = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "10.1.3.170,10.1.3.171,10.1.3.172")
    //      conf.set("hbase.zookeeper.property.clientPort", ConfigurationManager.getProperty(Constants.ZOOKEEPER_PORT))
    //      conf.set("hbase.zookeeper.quorum", ConfigurationManager.getProperty(Constants.ZOOKEEPER_HOST))
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val REGEX_MOBILE = "^((17[0-9])|(14[0-9])|(13[0-9])|(15[^4,\\D])|(18[0,5-9]))\\d{8}$"
    val valueRDD = hbaseRDD.map(result =>
      (Bytes.toString(result._2.getValue("basic".getBytes(), "cell_phone".getBytes())).toLong,
        Bytes.toString(result._2.getValue("basic".getBytes(), "other_cell_phone".getBytes())).toLong,
        Bytes.toString(result._2.getValue("basic".getBytes(), "init_type".getBytes()))))
      .filter(tuple3 => Pattern.matches(REGEX_MOBILE, tuple3._2.toString))
      .map(tuple3 => if (tuple3._3 == "被叫") (tuple3._2, tuple3._1) else (tuple3._1, tuple3._2)).distinct()

    val graph = Graph.fromEdgeTuples(valueRDD, 1)
    //      .partitionBy(PartitionStrategy.RandomVertexCut)
    //    val triCounts = graph.triangleCount().vertices
    //    triCounts.foreach(println)
    graph.edges.collect().foreach {
      case (Edge(src, dst, prop)) => println((src, dst, prop))
    }
  }


}
