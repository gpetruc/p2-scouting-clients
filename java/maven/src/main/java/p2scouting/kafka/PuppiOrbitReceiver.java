package p2scouting.kafka;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import p2scouting.core.ScoutingEventHeaderRecord;
/**
 * In this example, we implement a simple Kafka receiver that receives events or orbits and prints out their size
 */

public class PuppiOrbitReceiver {
    public static void main(String[] args) throws Exception {
        // Assign topicName to string variable
        String topicName = "test-puppi";
        if (args.length > 0) topicName = args[0];

        // create instance for properties to access producer configs
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Assign localhost id
        props.put("group.id", "test-puppi");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteBufferDeserializer");
        //props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<Long, ByteBuffer> consumer = new KafkaConsumer<Long, ByteBuffer>(props);

        // Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Arrays.asList(topicName));
        // print the topic name
        System.out.println("Subscribed to topic " + topicName);
        var pollInterval = Duration.ofMillis(1000);
        for (int emptyPolls = 0, maxEmpty = 10; emptyPolls < maxEmpty; ++emptyPolls) {
            ConsumerRecords<Long, ByteBuffer> records = consumer.poll(pollInterval);
            System.out.println("Poll returned "+records.count()+" records");
            if (!records.isEmpty()) emptyPolls = 0;
            for (ConsumerRecord<Long, ByteBuffer> record : records) {
                // print the offset,key and value for the consumer records.
                System.out.printf("offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value().limit());
                try {
                    LongBuffer lbuff = record.value().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
                    while (lbuff.hasRemaining()) {
                        long header = lbuff.get();
                        if (header == 0) continue;
                        ScoutingEventHeaderRecord eh = ScoutingEventHeaderRecord.decode(header);
                        if (eh.bx() < 25) {
                            System.out.printf("Event %016x run %d, orbit %d, bx %d, npuppi %d\n", header, eh.run(), eh.orbit(), eh.bx(), eh.nwords());
                        }
                        long[] evdata = new long[eh.nwords()];
                        lbuff.get(evdata);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
        System.out.println("End");
        consumer.close();
    }
}
