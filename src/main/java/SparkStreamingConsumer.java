import com.google.gson.Gson;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Milliseconds;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple5;

import java.io.Serializable;
import java.util.*;

public class SparkStreamingConsumer {
    public static final String KAFKA_TOPIC = "spark-streaming-topic";
    private static final Long TIME = 10 * 1000L; //milliseconds
    private static final int delayFactor = 10000; // 0..

    private static Gson gson = new Gson();

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf()
                .setAppName("BenchmarkSpark")
                .set("spark.streaming.backpressure.enabled","true")
                .setMaster("local");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Milliseconds.apply(TIME/2));

        jssc.checkpoint("_checkpoint");

        // see: http://spark.apache.org/docs/latest/streaming-kafka-integration.html
        Set<String> topicMap = new HashSet<>(Arrays.asList(KAFKA_TOPIC));
        Map<String, String> kafkaParams = new HashMap<String, String>() {
            {
                put("metadata.broker.list", "localhost:9092");
                put("auto.offset.reset", "smallest");
            }
        };

        JavaPairInputDStream<String, String> messages =
                KafkaUtils.createDirectStream(jssc,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        kafkaParams,
                        topicMap);

        messages
                .map(x -> {
                    Message message = gson.fromJson(x._2(), Message.class);

                    // time delay emulation
                    int count = 0;
                    byte[] array = message.getUid().getBytes();
                    for (int j = 0; j < delayFactor; j++) {
                        for (int i = 0; i < array.length; i++) {
                            if (array[0] == array[i]) count++;
                        }
                    }

                    return new Tuple5<String, Message, Long, Long, Integer>(
                            x._2(), message, System.currentTimeMillis(), 1L, count);
                })
                .window(Milliseconds.apply(TIME), Milliseconds.apply(TIME/2))
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
                            + "\nExpected time: " + Long.toString(TIME)
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
