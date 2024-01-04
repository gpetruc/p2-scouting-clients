package p2scouting.kafka;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * In this example, we implement a simple Kafka stream process that splits
 * orbits into events
 */
public class TimeSliceAggregator {
    private final static class MultipleTimesliceContainer implements Comparator<ByteBuffer>, Iterable<ByteBuffer> {
        private final ArrayList<ByteBuffer> timeslices;

        public MultipleTimesliceContainer() {
            timeslices = new ArrayList<ByteBuffer>();
        }


        public MultipleTimesliceContainer(ByteBuffer... vals) {
            timeslices = new ArrayList<ByteBuffer>();
            System.out.println("MTC created with "+vals.length+" buffers");
            for (var buff : Arrays.asList(vals))
                timeslices.add(buff);
        }

        public MultipleTimesliceContainer(MultipleTimesliceContainer other, ByteBuffer val) {
            timeslices = new ArrayList<ByteBuffer>(other.timeslices);
            timeslices.add(val);
        }

        ByteBuffer[] sortedArray() {
            Collections.sort(timeslices, this);
            ByteBuffer[] ret = new ByteBuffer[timeslices.size()];
            for (int i = 0; i < ret.length; ++i) {
                ret[i] = timeslices.get(i).duplicate().order(ByteOrder.LITTLE_ENDIAN);
            }
            return ret;
        }

        public int compare(ByteBuffer arg0, ByteBuffer arg1) {
            long bx0 = (arg0.getLong(0) >> 12) & 0x3FF;
            long bx1 = (arg1.getLong(0) >> 12) & 0x3FF;
            return (int) (bx0 - bx1);
        }

        @Override
        public Iterator<ByteBuffer> iterator() {
            return new MultipleTimesliceSplitterIterator(this);
        }

        byte[] serialize() {
            int nbytes = 4 * (timeslices.size() + 1);
            for (var ts : timeslices)
                nbytes += ts.limit();
            var ret = new byte[nbytes];
            var buf = ByteBuffer.wrap(ret).order(ByteOrder.LITTLE_ENDIAN);
            buf.putInt(0, timeslices.size());
            int offs = 4;
            for (var ts : timeslices) {
                buf.putInt(offs, ts.limit());
                offs += 4;
                ts.get(0, ret, offs, ts.limit());
                offs += ts.limit();
            }
            return ret;
        }

        static MultipleTimesliceContainer deserialize(byte[] data) {
            var buff = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
            var ret = new MultipleTimesliceContainer();
            int nitems = buff.getInt(), offs = 4;
            for (int i = 0; i < nitems; ++i) {
                int length = buff.getInt(offs);
                offs += 4;
                ret.timeslices.add(buff.slice(offs, length));
                offs += length;
            }
            return ret;
        }
    }

    public static class MultipleTimesliceSplitterIterator implements Iterator<ByteBuffer> {
        private final ByteBuffer[] buffers;
        private final int[] pointers, lengths;
        private int cursor;
        private boolean empty;

        public MultipleTimesliceSplitterIterator(MultipleTimesliceContainer container) {
            buffers = container.sortedArray();
            pointers = new int[this.buffers.length];
            lengths = new int[this.buffers.length];
            cursor = 0;
            empty = false;
            for (int i = 0; i < buffers.length; ++i) {
                ByteBuffer buff = buffers[i];
                pointers[i] = 0;
                lengths[i] = buff.limit();
                while (pointers[i] < lengths[i] && buff.getLong(pointers[i]) == 0)
                    pointers[i] += 8;
                if (pointers[i] == lengths[i])
                    empty = true;
            }
        }

        @Override
        public boolean hasNext() {
            return cursor != 0 || !empty;
        }

        @Override
        public ByteBuffer next() {
            ByteBuffer src = buffers[cursor];
            long header = src.getLong(pointers[cursor]);
            int npuppi = (int) (header & 0xFFF);
            ByteBuffer ret = src.slice(pointers[cursor], 8 * (npuppi + 1));
            pointers[cursor] += 8 * (npuppi + 1);
            while (pointers[cursor] < lengths[cursor] && src.getLong(pointers[cursor]) == 0)
                pointers[cursor] += 8;
            if (pointers[cursor] == lengths[cursor])
                empty = true;
            cursor = (cursor == buffers.length - 1 ? 0 : cursor + 1);
            return ret;
        }

    }

    private final static void usage() {
        System.out.println("Usage: PuppiOrbitSource [options] [--topic] topic ");
        System.out.println("Data generation options:");
        System.out.println("   -n:       number of timeslices to aggregate");
        System.out.println("Kafka options:");
        System.out.println("   --boostrap-server or -b: bootstrap server (default: localhost:9092)");
        System.exit(1);
     }
    public static void main(String[] args) {
        String topicName = null;
        String server = "localhost:9092";
        int nstreams = 2;
        try {
           for (int i = 0; i < args.length; ++i) {
              if (args[i].equals("--topic")) {
                 if (topicName != null)
                    usage();
                 topicName = args[++i];
              } else if (args[i].equals("-b") || args[i].equals("--bootstrap-server") || args[i].equals("--server")) {
                 server = args[++i];
              } else if (args[i].equals("-n")) {
                 nstreams = Integer.parseInt(args[++i]);
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

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-"+topicName);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteBuffer().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        Serde<MultipleTimesliceContainer> MTCSerDe = Serdes.serdeFrom(
            (topic, mtc) -> mtc.serialize(),
            (topic, bytes) -> MultipleTimesliceContainer.deserialize(bytes)
        );

        var topic1 = builder.<Long, ByteBuffer>stream(topicName+"-ts0");
        var topic2 = builder.<Long, ByteBuffer>stream(topicName+"-ts1");
        KStream<Long, MultipleTimesliceContainer> mergedStream;
        mergedStream = topic1.join(topic2,
                            (buff1, buff2) -> new MultipleTimesliceContainer(buff1, buff2),
                            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(1)));
        for (int istream = 2; istream < nstreams; ++istream) {
            var topic3 = builder.<Long, ByteBuffer>stream(topicName+"-ts"+istream);
            mergedStream = (KStream<Long, MultipleTimesliceContainer>) mergedStream.join(topic3,
                                (mtc, buff) -> new MultipleTimesliceContainer(mtc, buff),
                            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(1)),
                            StreamJoined.with(Serdes.Long(), MTCSerDe, Serdes.ByteBuffer()));
        }
        mergedStream.flatMapValues(mtc -> mtc)
                .to("test-puppi-events-split");

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
