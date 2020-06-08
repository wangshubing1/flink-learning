package examples.kafka;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @Author: king
 * @Date: 2019-08-02
 * @Desc: TODO
 */

public class KingKafkaProducer extends Thread {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;

    public KingKafkaProducer(String topic, Boolean isAsync) {
        this.topic = topic;
        this.isAsync = isAsync;

        Properties props= new Properties();
        props.put("bootstrap.servers", "szsjhl-cdh-test-10-9-251-30.belle.lan:9092," +
                "szsjhl-cdh-test-10-9-251-31.belle.lan:9092," +
                "szsjhl-cdh-test-10-9-251-32.belle.lan:9092");
        /*props.put ("acks","all");
        props.put ("retries", 0);
        props.put ("batch.size", 16384);
        props.put ("linger.ms", 1) ;
        props.put ("buffer.memory", 33554432);*/
        props.put("max.request.size", 10000000);
        props.put("buffer.memory", 10000000);
        props.put("batch.size", 10000000);
        props.put("client.id", "DemoProducer");
        props.put("receive.buffer.bytes", 10000000);
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<Integer, String>(props);
    }
    public void run(){
        Integer messageNo =1;

        while (true) {
            //String messageStr = "Message_" + messageNo;
            //File file = new File("kafka-producer/inputFile/ogg_test.json");
            try {
                //String contents = FileUtils.readFileToString(file, "UTF-8");

                String content ="{\"id\": "+messageNo+", \"type\": 0, \"name\":test"+messageNo+"}";
                long startTime = System.currentTimeMillis();
                if (isAsync) { // Send asynchronously
                    producer.send(new ProducerRecord<Integer, String>(topic, messageNo, content)
                            , new DemoCallBack(startTime, messageNo, content));
                    System.out.println("Sent message: (" + messageNo + ", " + content + ")");
                } else { // Send synchronously
                    try {
                        producer.send(new ProducerRecord<Integer, String>(topic, messageNo, content)).get();
                       System.out.println("Sent message: (" + messageNo + ", " + content + ")");
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            ++messageNo;

            try {
                sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
class DemoCallBack implements Callback {
        private final long startTime;
        private final int key;
        private final String message;

        public DemoCallBack(long startTime, int key, String message) {
            this.startTime = startTime;
            this.key = key;
            this.message = message;
        }

        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (recordMetadata != null) {
                /*System.out.println(
                        "message(" + key + ", " + message + ") sent to partition(" + recordMetadata.partition() +
                                "), " +
                                "offset(" + recordMetadata.offset() + ") in " + elapsedTime + " ms");*/
            } else {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {

        KingKafkaProducer producerThread = new KingKafkaProducer("king_ogg_test5", true);
        producerThread.start();
    }
}
