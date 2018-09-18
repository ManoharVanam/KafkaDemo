package com.manu.kafka.basic;

        import com.manu.kafka.largemessages.FileUtils;
        import org.apache.kafka.clients.consumer.*;
        import org.apache.kafka.clients.producer.KafkaProducer;
        import org.apache.kafka.common.serialization.StringDeserializer;

        import java.io.FileInputStream;
        import java.io.FileNotFoundException;
        import java.io.IOException;
        import java.util.*;


public class MyConsumer {

    static long start = System.currentTimeMillis();

    public static void main(String[] args) throws InterruptedException, IOException {
        String filePath = "/Users/mvanam/Documents/kafkaOutput/";
        String topic = "manu100";
        start = System.currentTimeMillis();
        Consumer<String, byte[]> consumer;
        try (FileInputStream fis = new FileInputStream("src/main/resources/consumer.props")) {
            Properties properties = new Properties();
            properties.load(fis);
            consumer = new KafkaConsumer<>(properties);
        }
        consumer.subscribe(Arrays.asList(topic));
        int i =0;
        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(1);
            for (ConsumerRecord<String, byte[]> record : records) {
//                        System.out.println(record);
//                Thread.sleep(1000);
//                        System.out.printf("Consumed record offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                FileUtils.writeFile(record.value(), filePath + "file-" + i++ + ".log");
//                        System.out.println("\n");
                long end = System.currentTimeMillis();
                System.out.println("Total time : " + (end - start) + "MilliSecs");
            }
            consumer.commitSync();

        }
//        consumer.close();
    }
}