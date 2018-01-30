import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, PartitionStrategy, VertexId}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2018/1/17.
  */
object OriginData {

  def main(args: Array[String]): Unit = {

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
        if (num.size != 0) {
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
      val graph = Graph.fromEdgeTuples(rdd, 1)
      //.partitionBy(PartitionStrategy.RandomVertexCut)
      val tc = graph.triangleCount().vertices
      val cc = graph.connectedComponents().vertices
      
      //      graph.vertices.collect().foreach(println(_))
      //      graph.pageRank(0.01).vertices.collect().foreach(println(_))
      /*graph.edges.collect().foreach {
        case (Edge(src, dst, prop)) => println((src, dst, prop))
      }*/
    } finally {
      //确保scanner关闭
      scanner.close()
    }
    conn.close()
    table.close()
    sc.stop()
  }
}
