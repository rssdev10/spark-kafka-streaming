# spark-kafka-streaming
Simple streaming test

#  How to run
  ```git clone https://github.com/rssdev10/spark-kafka-streaming.git```
  
  ```cd spark-kafka-streaming```

  # downloads kafka and zookeeper
  
  ```./gradlew setup```

  # run zookeeper, kafka, and run messages generation
  
  ```./gradlew test_data_prepare```

To break data generation press CTRL-C. And continue it by same command

```./gradlew test_data_prepare```

And in other console just run:

   ```./gradlew test_spark```

or

   ```./gradlew test_flink```

Stop all:
  ```./gradlew stop_all```

Spark test must generate messages each 10 seconds like:
```text
*************************************************************************** 
Processing time: 33477 
Expected time: 10000 
Processed messages: 2897866 
Message example: {"message": 1, "uid":"dde09b16-248b-4a2b-8936-109c72eb64cc"} 
Recovered json: {"message":1,"uid":"dde09b16-248b-4a2b-8936-109c72eb64cc"}
```

*message* is number of fist message in the window. Time values are in milliseconds.

# Additional parameters

[src/main/java/KafkaDataProducer.java](src/main/java/KafkaDataProducer.java)
```MESSAGES_NUMBER = 100L * 1000 * 1000;```

[src/main/java/SparkStreamingConsumer.java](src/main/java/SparkStreamingConsumer.java)
```delayFactor = 10000; // 0..```
