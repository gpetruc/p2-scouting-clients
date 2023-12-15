package p2scouting.kafka;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * In this example, we implement a simple Kafka producers that generates orbits with events containing random data
 */

public class PuppiOrbitSource {

   public static void generateOrbit(int orbit, int tmux, int offset, ByteBuffer buffer, Random rng) {
      buffer.clear();
      var blong = buffer.asLongBuffer();
      var bindex = 0;
      for (int bx = offset; bx < 3564; bx += tmux) {
         int npuppi = (int) Math.min(30*Math.exp(1.3*rng.nextGaussian()), 207);
         long header = ((long)npuppi) | (((long)bx) << 12 ) | (((long)orbit) << 24) | (0x01l << 62);
         blong.put(bindex, header);
         var rnddata = rng.longs(npuppi).toArray();
         blong.put(bindex+1, rnddata);
         bindex += 1+npuppi;
         if (bx < 25) System.out.printf("Orbit %d, bx %d, npuppi %d\n", orbit, bx, npuppi);
      }
      System.out.println("Generated one orbit with "+bindex+" words");
      buffer.flip();
      buffer.limit(bindex*8); // blong's limit doesn't propagate automatically
   }

   public static void main(String[] args) throws Exception {
      // Assign topicName to string variable
      String topicName = "test-puppi";

      // create instance for properties to access producer configs
      Properties props = new Properties();
      props.put("bootstrap.servers", "localhost:9092"); // Assign localhost id
      props.put("acks", "all"); // Set acknowledgements for producer requests.
      props.put("retries", 0);
      props.put("batch.size", 16384);
      props.put("linger.ms", 1);
      props.put("buffer.memory", 33554432);

      props.put("key.serializer",
            "org.apache.kafka.common.serialization.LongSerializer");

      props.put("value.serializer",
            "org.apache.kafka.common.serialization.ByteBufferSerializer");

      // make a sample of events
      int offset = 0;
      if (args.length > 0) offset = Integer.parseInt(args[0]);
      System.err.printf("Will produce data with a tmux offset %d (not to be confused with the kafka offset)\n", offset);
      var rng = new Random(37+offset);
      Producer<Long, ByteBuffer> producer = new KafkaProducer<Long, ByteBuffer>(props);

      for (int i = 0; i < 10; i++) {
         ByteBuffer buffer = ByteBuffer.allocate(4*1024*1024);
         int orbit = 100+i+1;
         generateOrbit(orbit, 6, offset, buffer, rng);
         var record = new ProducerRecord<>(topicName, (long)orbit, buffer);
         producer.send(record);
      }
      producer.close();
   }
}
