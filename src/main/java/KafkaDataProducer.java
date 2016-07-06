import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.*;

public class KafkaDataProducer {
    public static final Long MESSAGES_NUMBER = 100L * 1000 * 1000;

    private static final Random rand = new Random();

    private static final int uNum = 100;
    private static final List<String> uId = new ArrayList<String>() {
        private static final long serialVersionUID = -2650978174049138472L;
        {
            for (int i = 0; i < uNum; i++) {
                add(UUID.randomUUID().toString());
            }
        }
    };

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "localhost:9092");

        Producer<String,String> producer = new Producer<String, String>(new ProducerConfig(props));

        int number = 1;
        for(; number < MESSAGES_NUMBER; number++)
        {
            String messageStr =
                    String.format("{\"message\": %d, \"uid\":\"%s\"}",
                            number, uId.get(rand.nextInt(uNum)));

            producer.send(new KeyedMessage<String, String>(SparkStreamingConsumer.KAFKA_TOPIC,
                    null, messageStr));
            if (number % 10000 == 0)
                System.out.println("Messages pushed: " + number);
        }
        System.out.println("Messages pushed: " + number);
    }
}
