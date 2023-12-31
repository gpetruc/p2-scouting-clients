package p2scouting.kafka;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * In this example, we implement a simple Kafka producers that generates orbits
 * with events containing random data
 */

public class PuppiOrbitSource {

   public static void generateOrbit(int orbit, int tmux, int offset, ByteBuffer buffer, Random rng, int dumpbx) {
      buffer.clear();
      var blong = buffer.asLongBuffer();
      var bindex = 0;
      for (int bx = offset; bx < 3564; bx += tmux) {
         int npuppi = (int) Math.min(30 * Math.exp(1.3 * rng.nextGaussian()), 207);
         long header = ((long) npuppi) | (((long) bx) << 12) | (((long) orbit) << 24) | (2l << 62);
         blong.put(bindex, header);
         var rnddata = rng.longs(npuppi).toArray();
         blong.put(bindex + 1, rnddata);
         bindex += 1 + npuppi;
         if (bx < dumpbx)
            System.out.printf("Orbit %d, bx %d, header %016x, npuppi %d\n", orbit, bx, header, npuppi);
      }
      if (dumpbx >= 0)
         System.out.println("Generated one orbit with " + bindex + " words");
      buffer.flip();
      buffer.limit(bindex * 8); // blong's limit doesn't propagate automatically
   }

   public static void usage() {
      System.out.println("Usage: PuppiOrbitSource [options] [--topic] topic ");
      System.out.println("Data generation options:");
      System.out.println("   --tmux   or -T: tmux period");
      System.out.println("   --tslice or -t: tmux slice");
      System.out.println("   --seed   or -s: seed for the random number generator");
      System.out.println("   --firstOrbit  or -f: index of first orbit");
      System.out.println("Kafka options:");
      System.out.println("   --acks   or -a: acks to request: '0', '1', 'all' (default)");
      System.out.println("   --boostrap-server or -b: bootstrap server (default: localhost:9092)");
      System.exit(1);
   }

   public static void main(String[] args) throws Exception {
      // Assign topicName to string variable
      String topicName = null;
      String acks = "all";
      String server = "localhost:9092";
      int firstOrbit = 100, tmux = 6, tslice = 0, seed = 37, orbits = 10, dumpbx = 0;
      long bytes = 0;
      long tgen = 0, tsend = 0;
      try {
         for (int i = 0; i < args.length; ++i) {
            if (args[i].equals("--topic")) {
               if (topicName != null)
                  usage();
               topicName = args[++i];
            } else if (args[i].equals("-a") || args[i].equals("--acks")) {
               acks = args[++i];
            } else if (args[i].equals("-b") || args[i].equals("--bootstrap-server") || args[i].equals("--server")) {
               server = args[++i];
            } else if (args[i].equals("-T") || args[i].equals("--tmux")) {
               tmux = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-t") || args[i].equals("--tslice")) {
               tslice = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-s") || args[i].equals("--seed")) {
               seed = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-f") || args[i].equals("--firstOrbit")) {
               firstOrbit = Integer.parseInt(args[++i]);
            } else if (args[i].equals("--orbits")) {
               orbits = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-d") || args[i].equals("--dump")) {
               dumpbx = Integer.parseInt(args[++i]);
            } else {
               if (topicName != null)
                  usage();
               topicName = args[i];
            }
         }
         if (topicName == null)
            usage();
      } catch (Exception e) {
         e.printStackTrace();
         usage();
      }

      // create instance for properties to access producer configs
      Properties props = new Properties();
      props.put("bootstrap.servers", server); // Assign localhost id
      props.put("acks", acks); // Set acknowledgements for producer requests.
      props.put("retries", 0);
      props.put("batch.size", 16384);
      props.put("linger.ms", 1);
      props.put("buffer.memory", 33554432);

      props.put("key.serializer",
            "org.apache.kafka.common.serialization.LongSerializer");

      props.put("value.serializer",
            "org.apache.kafka.common.serialization.ByteBufferSerializer");

      // make a sample of events
      System.err.printf(
            "Will produce %d orbits of data with a tmux offset %d/%d (not to be confused with the kafka offset)\n",
            orbits, tslice, tmux);
      var rng = new Random(seed + tslice);
      Producer<Long, ByteBuffer> producer = new KafkaProducer<Long, ByteBuffer>(props);
      int npartitions = producer.partitionsFor(topicName).size();
      var runstart = System.currentTimeMillis();
      var orbit_ms = 3564/40e6*1000;
      for (int i = 0; i < orbits; i++) {
         var t0 = System.nanoTime();
         ByteBuffer buffer = ByteBuffer.allocate(4 * 1024 * 1024).order(ByteOrder.LITTLE_ENDIAN);
         int orbit = firstOrbit + i + 1;
         generateOrbit(orbit, tmux, tslice, buffer, rng, dumpbx);
         int partition = orbit % npartitions;
         long timestamp = runstart + (int)(orbit/orbit_ms);
         var t1 = System.nanoTime();
         var record = new ProducerRecord<>(topicName, partition, timestamp, (long) orbit, buffer);
         producer.send(record);
         var t2 = System.nanoTime();
         tgen += (t1 - t0);
         tsend += (t2 - t1);
         bytes += buffer.limit();
      }
      var t1 = System.nanoTime();
      producer.flush();
      var t2 = System.nanoTime();
      tsend += (t2 - t1);
      producer.close();
      double time_g = 1e-9 * (tgen), time_s = 1e-9 * (tsend), time = time_g + time_s;
      double inrate = bytes / (1024. * 1024.) / time;
      System.out.printf(
            "Sent %d orbits using %.1f ms (generate, %.1f kHz) + %.1f ms (send, %.1f kHz), tot+ %.1f ms (%.1f kHz, TM %.1f), output data rate %.1f MB/s (%.1f Gbps)\n",
            orbits,
            time_g,
            orbits / time_g / 1000.,
            time_s,
            orbits / time_s / 1000.,
            time,
            orbits / time / 1000.,
            (40e6 * time / 3564 / orbits),
            inrate,
            inrate * 8 / 1024.);
   }
}
