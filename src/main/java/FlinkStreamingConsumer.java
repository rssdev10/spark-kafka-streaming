import com.google.gson.Gson;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.io.Serializable;
import java.util.Properties;

public class FlinkStreamingConsumer {
    private static Gson gson = new Gson();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");

        props.put("group.id", "test");

        props.put("auto.offset.reset", "smallest");
        props.put("partition.assignment.strategy", "range");

        DataStream<String> dataStream = env
                .addSource(new FlinkKafkaConsumer08<String>(
                        SparkStreamingConsumer.KAFKA_TOPIC,
                        new SimpleStringSchema(), props));

        dataStream
                .map(json -> {
                    SparkStreamingConsumer.Message message = gson.fromJson(json, SparkStreamingConsumer.Message.class);

                    // time delay emulation
                    int count = 0;
                    byte[] array = message.getUid().getBytes();
                    for (int j = 0; j < SparkStreamingConsumer.delayFactor; j++) {
                        for (int i = 0; i < array.length; i++) {
                            if (array[0] == array[i]) count++;
                        }
                    }

                    return new DataTuple(json, message, System.currentTimeMillis(), 1L, count);
                }).returns(DataTuple.class)

                //.keyBy(x -> x.f1.getUid())
                .keyBy(x -> 1) // only one partition

                .timeWindow(Time.milliseconds(SparkStreamingConsumer.TIME))
                .reduce((x1, x2) -> new DataTuple(
                        x1.f0,
                        x1.f1,
                        x1.f2,
                        x1.f3 + x2.f3,
                        x1.f4))
                .map(x -> {
                    return "***************************************************************************"
                            + "\nProcessing time: " + Long.toString(System.currentTimeMillis() - x.f2)
                            + "\nExpected time: " + Long.toString(SparkStreamingConsumer.TIME)
                            + "\nProcessed messages: " + Long.toString(x.f3)
                            + "\nMessage example: " + x.f0
                            + "\nRecovered json: " + x.f1
                            ;
                })
                .print();

        env.execute();
    }
    public static class DataTuple
            extends Tuple5<String, SparkStreamingConsumer.Message, Long, Long, Integer> {

        public DataTuple() {
        }

        public DataTuple(String value0, SparkStreamingConsumer.Message value1, Long value2, Long value3, Integer value4) {
            super(value0, value1, value2, value3, value4);
        }
    }
}
