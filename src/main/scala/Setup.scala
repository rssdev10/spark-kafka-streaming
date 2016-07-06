import java.io.File
import java.net.URL
import java.util.Calendar

import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._

object Setup {

  private val TIME_OF_TEST: Int = 120 * 1000 /* in ms */

  private val apacheMirror = getApacheMirror

  private val curDir = System.getProperty("user.dir")


  private val inputTopic = SparkStreamingConsumer.KAFKA_TOPIC
  private val ZK_CONNECTIONS = "localhost:2181"

  //println("ls -l" !)
  object Product extends Enumeration {
    val kafka = "kafka"
    val zookeeper = "zookeeper"
    val scala_bin = "scala"
    val spark = "spark"
  }
  import Product._

  val VER = Map(
    kafka -> "0.8.2.2",
    scala_bin -> "2.11",
    spark -> "1.6.1",
    zookeeper -> "3.4.8"
  ).map {case (k,v) => (k, scala.util.Properties.envOrElse(k, v))}

  val products: Map[String, Product] = Map(
    kafka -> new Product(
      s"""kafka_${VER(scala_bin)}-${VER(kafka)}""",
      s"""kafka_${VER(scala_bin)}-${VER(kafka)}.tgz""",
      s"""$apacheMirror/kafka/${VER(kafka)}""") {
      override def start: Unit = {
        val PARTITIONS = 1

        startIfNeeded("kafka.Kafka", kafka, 10,
          s"$dirName/bin/kafka-server-start.sh", s"$dirName/config/server.properties")

        val count = s"""$dirName/bin/kafka-topics.sh --describe --zookeeper $ZK_CONNECTIONS --topic $inputTopic 2>/dev/null""" #| s"grep -c $inputTopic" !

        if (count.toInt == 0) {
          s"""$dirName/bin/kafka-topics.sh --create --zookeeper $ZK_CONNECTIONS --replication-factor 1 --partitions $PARTITIONS --topic $inputTopic""" !
        } else {
          println(s"Kafka topic $inputTopic already exists")
        }
      }

      override def stop: Unit = {
        stopIfNeeded( "kafka.Kafka", kafka)
        "rm -rf /tmp/kafka-logs/" !
      }
    },

    zookeeper -> new Product(
      s"""zookeeper-${VER(zookeeper)}""",
      s"""zookeeper-${VER(zookeeper)}.tar.gz""",
      s"""$apacheMirror/zookeeper/zookeeper-${VER(zookeeper)}""") {
      override def start: Unit = {
        startIfNeeded("org.apache.zookeeper.server", zookeeper, 10, s"$dirName/bin/zkServer.sh", "start")
      }

      override def stop: Unit = {
        stopIfNeeded("org.apache.zookeeper.server", zookeeper)
        "rm -rf /tmp/zookeeper" !
      }

      override def config(phase:String): Unit = {
        Process(Seq("bash","-c",s"""cp -f conf/zookeeper/* $dirName/conf""")).!;
      }
    }
  )

  val scenario: Map[String, () => Unit] = Map(
    "setup" -> (() => {
      //println(apacheMirror)
      products.foreach { case (k, v) => if (v.urlPath.nonEmpty) v.downloadAndUntar() }
      products.foreach { case (k, v) =>
        try {
          v.config("setup")
        } catch {
          case _: Throwable =>
        }
      }
    }),

    "test_data_prepare" -> (() => {
      val seq = Array(
        zookeeper,
        kafka
      )
      start(seq)

      KafkaDataProducer.main(null)
    }),

    "stop_all" -> (() => {
      val seq = Array(
        zookeeper,
        kafka
      )
      stop(seq)
    })
  )

  def main(args: Array[String]) {
    if (args.length > 0 && scenario.contains(args(0))) {
      scenario(args(0)).apply()
    } else {
      println(args.mkString(" "))
      println(
        """
          |Select command:
          | setup - download and unpack all files
          | ...
          | """.stripMargin)
    }
    System.exit(0);
  }

  private def getApacheMirror: String = {
    val str = Source.fromURL("https://www.apache.org/dyn/closer.cgi").mkString
    """<strong>(.+)</strong>""".r.findFirstMatchIn(str).get.group(1)
  }

  def pidBySample(sample: String): String = try {
    ("ps -aef" !!).split("\n").find(str => str.contains(sample)).head.split(" ").filter(_.nonEmpty).apply(1)
  } catch {
    case _: Throwable => ""
  }

  def startIfNeeded(sample: String, name: String, sleepTime:Integer, args: String*): Unit = {
    val pid = pidBySample(sample)
    if (pid.nonEmpty) {
      println( name + " is already running...")
    } else {
      args.mkString(" ").run()
      Thread sleep(sleepTime * 1000)
    }
  }

  def stopIfNeeded(sample: String, name: String): Unit = {
    val pid = pidBySample(sample)
    if (pid.nonEmpty) {
      s"""kill $pid""".run()

      Thread sleep 1000

      val again = pidBySample(sample)
      if (again.nonEmpty) {
        s"""kill -9 $pid""".run()
      }
    } else {
      println("No $name instance found to stop")
    }
  }

  def start(seq: Seq[String]) = {
    seq.foreach(x => {
      println(Calendar.getInstance.getTime + ": " + x + " starting ******************")
      products(x).start
    })
  }

  def stop(seq: Seq[String]) = {
    seq.reverse.foreach(products(_).stop)
  }

  abstract class Product(val dirName: String, val fileName: String, val urlPath: String){
    def downloadAndUntar() = {
      val localFile = s"download-cache/$fileName"
      val url = urlPath + "/" + fileName

      println(s"Download $url")
      println(s"Saving to $fileName")

      val file = new File(localFile)
      val exists = file.exists()
      if (exists && file.length() > 1024) {
        // check minimal size on case of HTTP-redirection with saving of non zero file
        println(s"Using cached File $fileName")
      } else {
        if (exists) file.delete()
        else new File("download-cache").mkdir

        new URL(url) #> new File(localFile) !
      }

      val tar = "tar -xzvf " + curDir + "/" + localFile
      tar !
    }

    def config(phase:String) : Unit = {}

    def start : Unit = {}
    def stop : Unit = {}
  }
}
