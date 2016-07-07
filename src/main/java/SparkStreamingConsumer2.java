import com.google.gson.Gson;
import consumer.kafka.MessageAndMetadata;
import consumer.kafka.ReceiverLauncher;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Milliseconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple5;

import java.io.Serializable;
import java.util.Properties;

public class SparkStreamingConsumer2 {
    private static Gson gson = new Gson();

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf()
                .setAppName("BenchmarkSpark")
                .set("spark.streaming.backpressure.enabled","true")
                // uncomment it to set physical limits of processing
                // .set("spark.streaming.receiver.maxRate", "10000")
                // .set("spark.streaming.kafka.maxRatePerPartition", "10000")
                .setMaster("local");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Milliseconds.apply(SparkStreamingConsumer.TIME/2));

        jssc.checkpoint("_checkpoint");

        // https://github.com/dibbhatt/kafka-spark-consumer
        Properties props = new Properties();
        props.put("zookeeper.hosts", "localhost");
        props.put("zookeeper.port", "2181");
        props.put("zookeeper.broker.path", "/brokers");
        props.put("kafka.topic", SparkStreamingConsumer.KAFKA_TOPIC);
        props.put("kafka.consumer.id", "test");
        props.put("zookeeper.consumer.connection", "localhost:2181");
        props.put("zookeeper.consumer.path", "/consumer-path");
        //Optional Properties
        props.put("consumer.forcefromstart", "true");
        props.put("consumer.fetchsizebytes", "1048576");
        props.put("consumer.fillfreqms", "250");
        props.put("consumer.backpressure.enabled", "true");

        //props.put("auto.offset.reset", "smallest");
        props.put("auto.offset.reset", "largest");

        //Specify number of Receivers you need.
        //It should be less than or equal to number of Partitions of your topic

        int numberOfReceivers = 1;

        JavaDStream<MessageAndMetadata> messages =
                ReceiverLauncher.launch(jssc, props, numberOfReceivers, StorageLevel.MEMORY_ONLY());


        messages
                .map(x -> {
                    String json = new String(x.getPayload());
                    Message message = gson.fromJson(json, Message.class);

                    // time delay emulation
                    int count = 0;
                    byte[] array = message.getUid().getBytes();
                    for (int j = 0; j < SparkStreamingConsumer.delayFactor; j++) {
                        for (int i = 0; i < array.length; i++) {
                            if (array[0] == array[i]) count++;
                        }
                    }

                    return new Tuple5<String, Message, Long, Long, Integer>(
                            json, message, System.currentTimeMillis(), 1L, count);
                })
                .window(Milliseconds.apply(SparkStreamingConsumer.TIME))
                .reduce((x1, x2) -> new Tuple5<String, Message, Long, Long, Integer>(
                        x1._1(),
                        x1._2(),
                        x1._3(),
                        x1._4() + x2._4(),
                        x1._5()))
                .map(x -> {
                    System.out.println(
                            "***************************************************************************"
                            + "\nProcessing time: " + Long.toString(System.currentTimeMillis() - x._3())
                            + "\nExpected time: " + Long.toString(SparkStreamingConsumer.TIME)
                            + "\nProcessed messages: " + Long.toString(x._4())
                            + "\nMessage example: " + x._1()
                            + "\nRecovered json: " + x._2()
                    );
                    return x;
                })
                .dstream().saveAsTextFiles("/tmp/spark-test", "spark-streaming");

        jssc.start();
        jssc.awaitTermination();
    }

    public class Message implements Serializable {
        private Long message;
        private String uid;

        public Long getMessage() {
            return message;
        }

        public void setMessage(Long message) {
            this.message = message;
        }

        public String getUid() {
            return uid;
        }

        public void setUid(String uid) {
            this.uid = uid;
        }

        public String toString() {
            return gson.toJson(this);
        }
    }
}
