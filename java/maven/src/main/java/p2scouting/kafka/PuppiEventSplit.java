package p2scouting.kafka;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Produced;

import p2scouting.core.PuppiSOAFloatRecord;
import p2scouting.core.ScoutingEventHeaderRecord;
import p2scouting.kafka.serdes.PuppiFloatEventDeserializer;
import p2scouting.kafka.serdes.PuppiFloatEventSerializer;
import p2scouting.kafka.serdes.ScoutingEventHeaderDeserializer;
import p2scouting.kafka.serdes.ScoutingEventHeaderSerializer;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * In this example, we implement a simple Kafka stream process that splits orbits into events
 */
public class PuppiEventSplit {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-puppi-evsplit");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteBuffer().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        final Serde<ScoutingEventHeaderRecord> headerSerDe = new Serdes.WrapperSerde<>(new ScoutingEventHeaderSerializer(), new ScoutingEventHeaderDeserializer());
        final Serde<PuppiSOAFloatRecord> floatsSerDe = new Serdes.WrapperSerde<>(new PuppiFloatEventSerializer(), new PuppiFloatEventDeserializer());

        builder.<Long, ByteBuffer>stream("test-puppi").flatMap(
            new PuppiEventSplitterKernel()).map(
            new PuppiEventUnpackerKernel()).to("test-puppi-unpacked", Produced.with(headerSerDe, floatsSerDe));

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
