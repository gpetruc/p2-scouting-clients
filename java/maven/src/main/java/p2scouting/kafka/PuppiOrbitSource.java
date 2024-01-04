package p2scouting.kafka;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * In this example, we implement a simple Kafka producers that generates orbits
 * with events containing random data
 */

public class PuppiOrbitSource {

   private static interface OrbitGenerator {
      public abstract void generateOrbit(int orbit, int tmux, int tslice, ByteBuffer buffer, int dumpbx);
   }

   private static final class RngOrbitGenerator implements OrbitGenerator {
      private final Random rng;

      public RngOrbitGenerator(Random rng) {
         this.rng = rng;
      }

      public void generateOrbit(int orbit, int tmux, int tslice, ByteBuffer buffer, int dumpbx) {
         buffer.clear();
         var blong = buffer.asLongBuffer();
         var bindex = 0;
         // var rnddata_fix = rng.longs(208).toArray();
         for (int bx = tslice; bx < 3564; bx += tmux) {
            int npuppi = (int) Math.min(30 * Math.exp(1.3 * rng.nextGaussian()), 207);
            long header = ((long) npuppi) | (((long) bx) << 12) | (((long) orbit) << 24) | (2l << 62);
            blong.put(bindex, header);
            var rnddata = rng.longs(npuppi).toArray();
            blong.put(bindex + 1, rnddata);
            // blong.put(bindex + 1, rnddata_fix, 0, npuppi);
            bindex += 1 + npuppi;
            if (bx < dumpbx)
               System.out.printf("Orbit %d, bx %d, header %016x, npuppi %d\n", orbit, bx, header, npuppi);
         }
         if (dumpbx >= 0)
            System.out.println("Generated one orbit with " + bindex + " words");
         buffer.flip();
         buffer.limit(bindex * 8); // blong's limit doesn't propagate automatically
      }
   }

   private static final class DummyOrbitGenerator implements OrbitGenerator {
      private final short[] npuppi;
      private final long[] rnddata;

      public DummyOrbitGenerator(Random rng) {
         npuppi = new short[3564];
         for (int i = 0; i < npuppi.length; ++i) {
            npuppi[i] = (short) Math.min(30 * Math.exp(1.3 * rng.nextGaussian()), 207);
         }
         rnddata = rng.longs(208).toArray();
      }

      public void generateOrbit(int orbit, int tmux, int tslice, ByteBuffer buffer, int dumpbx) {
         buffer.clear();
         var blong = buffer.asLongBuffer();
         var bindex = 0;
         for (int bx = tslice; bx < 3564; bx += tmux) {
            long header = ((long) npuppi[bx]) | (((long) bx) << 12) | (((long) orbit) << 24) | (2l << 62);
            blong.put(bindex, header);
            blong.put(bindex + 1, rnddata, 0, npuppi[bx]);
            bindex += 1 + npuppi[bx];
         }
         buffer.flip();
         buffer.limit(bindex * 8); // blong's limit doesn't propagate automatically
      }
   }

   private static final class PuppiStreamGenerator implements Runnable {
      private final int firstOrbit, tmux, tslice, orbits;
      private final OrbitGenerator generator;
      private final Producer<Long, ByteBuffer> producer;
      private final String topicName;
      private final int partitions;
      private final long runstart;
      private final boolean pretend;
      private int debug = -1;
      private long bytes = 0;
      private long tgen = 0, tsend = 0;

      public PuppiStreamGenerator(int tmux, int tslice, int orbits, int firstOrbit, OrbitGenerator orbitGen,
            Producer<Long, ByteBuffer> producer, String topicName, int partitions, long runstart) {
         this.tmux = tmux;
         this.tslice = tslice;
         this.orbits = orbits;
         this.firstOrbit = firstOrbit;
         this.generator = orbitGen;
         this.producer = producer;
         this.topicName = topicName;
         this.partitions = partitions;
         this.runstart = runstart;
         this.pretend = (producer == null);
      }

      public final void setDebug(int debug) {
         this.debug = debug;
      }

      @Override
      public void run() {
         var orbit_ms = 3564 / 40e6 * 1000;
         ByteBuffer buffer = ByteBuffer.allocate(4 * 1024 * 1024).order(ByteOrder.LITTLE_ENDIAN);
         for (int i = 0; i < orbits; i++) {
            var t0 = System.nanoTime();
            int orbit = firstOrbit + i + 1;
            generator.generateOrbit(orbit, tmux, tslice, buffer, debug);
            bytes += buffer.limit();
            int partition = orbit % partitions;
            long timestamp = runstart + (int) (orbit / orbit_ms);
            var t1 = System.nanoTime();
            tgen += (t1 - t0);
            var data = new byte[buffer.limit()];
            buffer.get(data);
            var record = new ProducerRecord<>(topicName, partition, timestamp, (long) orbit, ByteBuffer.wrap(data));
            if (pretend)
               continue;
            producer.send(record);
            var t2 = System.nanoTime();
            tsend += (t2 - t1);
         }

      }

      void printSummary(long tclose) {
         tsend += tclose;
         double time_g = 1e-9 * (tgen), time_s = 1e-9 * (tsend), time = time_g + time_s;
         double inrate = bytes / (1024. * 1024.) / time;
         System.out.printf(
               "Sent %d orbits using %.1f ms (generate, %.1f kHz) + %.1f ms (send, %.1f kHz), tot %.1f ms (%.1f kHz, TM %.1f), output data rate %.1f MB/s (%.1f Gbps)\n",
               orbits,
               time_g,
               orbits / time_g / 1000.,
               time_s,
               pretend ? 0 : orbits / time_s / 1000.,
               time,
               orbits / time / 1000.,
               (40e6 * time / 3564 / orbits),
               inrate,
               inrate * 8 / 1024.);
      }
   }

   private final static void usage() {
      System.out.println("Usage: PuppiOrbitSource [options] [--topic] topic ");
      System.out.println("Data generation options:");
      System.out.println("   -n:       number of parallel streams to generate");
      System.out.println("   --orbits: number of orbits to generate");
      System.out.println("   --time:   number of seconds to generate (exclusive with --orbits)");
      System.out.println("   --tmux   or -T: tmux period");
      System.out.println("   --tslice or -t: tmux slice");
      System.out.println("   --seed   or -s: seed for the random number generator");
      System.out.println("   --firstOrbit  or -f: index of first orbit");
      System.out.println("   --algo   or -A: orbit generation algorithm ()");
      System.out.println("Kafka options:");
      System.out.println("   --pretend or -p: don't send data at all");
      System.out.println("   --acks   or -a: acks to request: '0', '1', 'all' (default)");
      System.out.println("   --boostrap-server or -b: bootstrap server (default: localhost:9092)");
      System.exit(1);
   }

   public static void main(String[] args) throws Exception {
      String algo = "rng";
      String topicName = null;
      String acks = "all";
      String server = "localhost:9092";
      int firstOrbit = 100, tmux = 6, tslice = 0, seed = 37, orbits = 0, dumpbx = -1, seconds = 0, nstreams = 1;
      boolean pretend = false;
      try {
         for (int i = 0; i < args.length; ++i) {
            if (args[i].equals("--topic")) {
               if (topicName != null)
                  usage();
               topicName = args[++i];
            } else if (args[i].equals("-A") || args[i].equals("--algo")) {
               algo = args[++i];
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
            } else if (args[i].equals("--time")) {
               seconds = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-n")) {
               nstreams = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-p") || args[i].equals("--pretend")) {
               pretend = true;
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
         if (seconds != 0 && orbits != 0)
            usage();
         else if (seconds != 0)
            orbits = (int) Math.ceil((40e6 / 3564) * seconds);
         else if (orbits == 0)
            orbits = 10;
         seconds = Math.max(seconds, (int) Math.ceil(orbits / (40e6 / 3564)));
         if (!algo.equals("rng") && !algo.equals("dummy"))
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
      props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
      props.put("value.serializer", "org.apache.kafka.common.serialization.ByteBufferSerializer");

      Producer<Long, ByteBuffer> producer = pretend ? null : new KafkaProducer<Long, ByteBuffer>(props);
      int npartitions = producer != null ? producer.partitionsFor(topicName).size() : 1;
      var runstart = System.currentTimeMillis();
      var tasks = new ArrayList<PuppiStreamGenerator>();
      var pool = Executors.newFixedThreadPool(nstreams);
      for (int istream = 0; istream < nstreams; ++istream) {
         int mytslice = tslice + istream;
         OrbitGenerator orbitGen = null;
         if (algo.equals("rng")) {
            orbitGen = new RngOrbitGenerator(new Random(seed + mytslice));
         } else if (algo.equals("dummy")) {
            orbitGen = new DummyOrbitGenerator(new Random(seed + mytslice));
         }
         var myTopicName = (nstreams == 1) ? topicName : (topicName + "-ts" + istream);
         var task = new PuppiStreamGenerator(tmux, mytslice, orbits, firstOrbit, orbitGen, producer, myTopicName,
               npartitions, runstart);
         if (dumpbx != -1)
            task.setDebug(dumpbx);
         System.err.printf("Will produce %d orbits of data with a tmux offset %d/%d\n", orbits, mytslice, tmux);
         tasks.add(task);
      }
      long tclose = -1;
      try {
         for (var task : tasks) {
            pool.submit(task);
         }
         pool.shutdown();
         pool.awaitTermination(seconds * 30, TimeUnit.SECONDS);
         if (!pool.isTerminated()) {
            System.out.println("Error, timeout waiting for tasks to finish.");
         } else {
            System.out.println("All tasks done, closing the producer.");
            if (producer != null) {
               var t1 = System.nanoTime();
               producer.flush();
               var t2 = System.nanoTime();
               tclose += (t2 - t1);
               producer.close();
            } else {
               tclose = 0;
            }
         }
      } catch (Exception e) {
         e.printStackTrace();
         pool.shutdownNow();
      }
      if (tclose != -1) {
         for (var task : tasks) {
            task.printSummary(tclose);
         }
      }
   }
}
