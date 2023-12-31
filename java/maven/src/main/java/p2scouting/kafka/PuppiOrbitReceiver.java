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
 * In this example, we implement a simple Kafka receiver that receives events or
 * orbits and prints out their size
 */

public class PuppiOrbitReceiver {
    public static void usage() {
        System.out.println("Usage: PuppiOrbitReceiver [options] [--topic] topic ");
        System.out.println("Data generation options:");
        System.out.println("   --split  or -s: split orbits to count events, bxs");
        System.out.println("   --dump   or -d: dump (-1 = none, 0 = orbits, N = first N bx)");
        System.out.println("Kafka options:");
        System.out.println("   --poll   or -p: poll interval in ms");
        System.out.println("   --boostrap-server or -b: bootstrap server (default: localhost:9092)");
        System.out.println("   --group-id or -g: group id (defaults to 'read-'+topicName)");
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        // Assign topicName to string variable
        String topicName = null, groupName = null;
        var pollInterval = Duration.ofMillis(1000);
        int maxEmpty = 10;
        String server = "localhost:9092";
        boolean split = false;
        int dumpbx = 0;
        int orbits = 0, lastorbit = -1;
        long messages = 0, bytes = 0, events = 0, candidates = 0;
        long t0 = -1, t1 = -1;
        try {
            for (int i = 0; i < args.length; ++i) {
                if (args[i].equals("--topic")) {
                    if (topicName != null)
                        usage();
                    topicName = args[++i];
                } else if (args[i].equals("-g") || args[i].equals("--group-id")) {
                    groupName = args[++i];
                } else if (args[i].equals("-b") || args[i].equals("--bootstrap-server") || args[i].equals("--server")) {
                    server = args[++i];
                } else if (args[i].equals("-p") || args[i].equals("--poll")) {
                    pollInterval = Duration.ofMillis(Integer.parseInt(args[++i]));
                    maxEmpty = (int) Math.ceil(10000 / pollInterval.toMillis());
                } else if (args[i].equals("-s") || args[i].equals("--split")) {
                    split = true;
                } else if (args[i].equals("-d") || args[i].equals("--dump")) {
                    dumpbx = Integer.parseInt(args[++i]);
                    if (dumpbx > 0)
                        split = true;
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
        props.put("group.id", groupName != null ? groupName : "read-" + topicName);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteBufferDeserializer");

        KafkaConsumer<Long, ByteBuffer> consumer = new KafkaConsumer<Long, ByteBuffer>(props);

        // Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Arrays.asList(topicName));
        // print the topic name
        System.out.println("Subscribed to topic " + topicName + " from " + server);

        for (int emptyPolls = 0; emptyPolls < maxEmpty; ++emptyPolls) {
            ConsumerRecords<Long, ByteBuffer> records = consumer.poll(pollInterval);
            System.out.println("Poll returned " + records.count() + " records");
            if (!records.isEmpty())
                emptyPolls = 0;
            for (ConsumerRecord<Long, ByteBuffer> record : records) {
                if (t0 == -1)
                    t0 = System.nanoTime();
                long key = record.key();
                if (key != lastorbit) {
                    orbits++;
                }
                messages++;
                bytes += record.value().limit();
                if (dumpbx >= 0)
                    System.out.printf("offset = %d, key = %s, value = %s\n",
                            record.offset(), record.key(), record.value().limit());
                if (split) {
                    try {
                        LongBuffer lbuff = record.value().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
                        while (lbuff.hasRemaining()) {
                            long header = lbuff.get();
                            if (header == 0)
                                continue;
                            ScoutingEventHeaderRecord eh = ScoutingEventHeaderRecord.decode(header);
                            if (eh.bx() < dumpbx) {
                                System.out.printf("Event %016x run %d, orbit %d, bx %d, npuppi %d\n", header, eh.run(),
                                        eh.orbit(), eh.bx(), eh.nwords());
                            }
                            long[] evdata = new long[eh.nwords()];
                            lbuff.get(evdata);
                            events++;
                            candidates += eh.nwords();
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
                t1 = System.nanoTime();
            }
        }
        var time = (1e-9 * (t1 - t0));
        double inrate = bytes / (1024. * 1024.) / time;
        if (!split) {
            System.out.printf(
                    "Done in %.2fs. Processed %d messages (rate: %.1f kHz), %d orbits (rate: %.1f kHz, i.e. TM %.1f), input data rate %.1f MB/s (%.1f Gbps)\n",
                    time,
                    messages,
                    messages / time / 1000.,
                    orbits,
                    orbits / time / 1000.,
                    (40e6 * time / 3564 / orbits),
                    inrate,
                    inrate * 8 / 1024.);
        } else {
            System.out.printf(
                    "Done in %.2fs. Processed %d messages (rate: %.1f kHz), %d orbits (rate: %.1f kHz, i.e. TM %.1f), %d events, %d candidates\nEvent rate: %.1f kHz (40 MHz / %.1f), input data rate %.1f MB/s (%.1f Gbps)\n",
                    time,
                    messages,
                    messages / time / 1000.,
                    orbits,
                    orbits / time / 1000.,
                    (40e6 * time / 3564 / orbits),
                    events, candidates,
                    events / time / 1000.,
                    (40e6 * time / events),
                    inrate,
                    inrate * 8 / 1024.);
        }
        consumer.close();
    }
}
