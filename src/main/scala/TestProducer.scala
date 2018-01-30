import java.io.File
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object TestProducer {

  def main(args: Array[String]): Unit = {
    val props = new Properties
    props.put("bootstrap.servers", "10.1.3.172:9092,10.1.3.171:9092,10.1.3.172:9092")
    //The "all" setting we have specified will result in blocking on the full commit of the record, the slowest but most durable setting.
    //“所有”设置将导致记录的完整提交阻塞，最慢的，但最持久的设置。
    props.put("acks", "all")
    //如果请求失败，生产者也会自动重试，即使设置成０ the producer can automatically retry.
    props.put("retries", "0")

    //The producer maintains buffers of unsent records for each partition.
    props.put("batch.size", 16384.toString)
    //默认立即发送，这里这是延时毫秒数
    props.put("linger.ms", 1.toString)
    //生产者缓冲大小，当缓冲区耗尽后，额外的发送调用将被阻塞。时间超过max.block.ms将抛出TimeoutException
    props.put("buffer.memory", 33554432.toString)
    //The key.serializer and value.serializer instruct how to turn the key and value objects the user provides with their ProducerRecord into bytes.
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    //文件夹
    val file = new File("D://datatest/ori_data/ori_data/result-1000")
    val fileNameArr = getFileName(file)
    var i = 1
    for (elem <- fileNameArr) {
      val name = elem.toString.replace("\\", "\\\\")
      val source = Source.fromFile(name)
      if (source.getLines() != null) {
        val res = source.getLines()
        res.foreach(x => {
          producer.send(new ProducerRecord[String, String]("play", x))
          println(x)
          println("已发送:" + i + "条")
          i = i + 1
          Thread.sleep(1000)
        })
      }
      source.close()
      /*producer.send(new ProducerRecord[String, String]("play", source.mkString))
      println("已发送:" + i * 10 + "条")
      i = i + 1
      Thread.sleep(20000)*/
    }
    producer.close()
  }
//返回文件夹下的*.txt 文件
  def getFileName(file: File): Array[File] = {
    val fileNameArr = file.listFiles().filter(!_.isDirectory)
      .filter(x => x.toString.endsWith(".txt"))
    fileNameArr ++ file.listFiles().filter(_.isDirectory).flatMap(getFileName)
  }
}
