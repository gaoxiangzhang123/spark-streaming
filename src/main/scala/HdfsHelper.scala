import java.io.{File, FileWriter}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2018/1/12.
  */
object HdfsHelper {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    val data = spark.sparkContext.textFile("hdfs://10.1.3.171:8020/tmp/")
    data.foreach(x=>{
    })
    test3()
  }
  def test3(): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    //    readFile
    //      test()
    val arr = ArrayBuffer[Path]()
    val a = getName(new Path("/app/graph"), arr)
    /*println(a(1))
    println(a.length)*/
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    //目录配置namenode节点
    val data = sc.textFile(a(1).toString)
    //    val res = data.filter(x=>{!(x.contains(",0")||x.contains(",1"))})
    data.foreach(println(_))
    //    res.saveAsTextFile("D://datatest/res")
    sc.stop()
  }

def getName(path:Path,arr:ArrayBuffer[Path]): ArrayBuffer[Path] ={
//  System.setProperty("HADOOP_USER_NAME", "hdfs")
  val conf = new Configuration()
  conf.set("fs.defaultFS", "hdfs://bigdata08171:8020")
  //    conf.set("mapred.remote.os", "Linux")
  val fileSystem = FileSystem.get(conf)
  val list = fileSystem.listStatus(path)
  for (res <- list) {
    if (res.isDirectory()) {
//      println(res.getPath)
      getName(res.getPath,arr)
    } else {
      arr.append(res.getPath)
    }
  }
  arr
}
  def test2(): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(sparkConf)

  }
  def readFile: Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    //目录配置namenode节点
    val data = sc.textFile("hdfs://bigdata08171:8020/app/graph/1516102206338/13000000789/part-00000")
//    val res = data.filter(x=>{!(x.contains(",0")||x.contains(",1"))})
    data.foreach(println(_))
//    res.saveAsTextFile("D://datatest/res")
    sc.stop()
  }


  def test(): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://bigdata08171:8020")
    //    conf.set("mapred.remote.os", "Linux")
    val fileSystem = FileSystem.get(conf)
    val list = fileSystem.listStatus(new Path("/app/graph/1516102206338/13000000789"))
    for (res <- list) {
      if (res.isDirectory()) {
        println(res.getPath)
      } else {
        println(res.getPath)
      }
    }


  }

}
