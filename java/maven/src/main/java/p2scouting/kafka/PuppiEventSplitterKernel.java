package p2scouting.kafka;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

/**
 * In this example, we implement a simple Kafka stream kernel that splits orbits into events
 */

public class PuppiEventSplitterKernel
        implements KeyValueMapper<Long, ByteBuffer, Iterable<KeyValue<Long, ByteBuffer>>> {

    @Override
    public Iterable<KeyValue<Long, ByteBuffer>> apply(Long key, ByteBuffer value) {
        System.err.println("Asked to split key " + key +", a byte buffer of length "+value.limit());
        value.order(ByteOrder.LITTLE_ENDIAN);
        List<KeyValue<Long, ByteBuffer>> result = new ArrayList<>();
        for (int offs = 0, tail = value.limit(); offs < tail; ) {
            long header = value.getLong(offs);
            if (header == 0) {
                offs += 8;
                continue;
            }
            long evkey = header >> 12;
            int npuppi = (int)(header & 0xFFF);
            int length = 8*(npuppi+1);
            var evdata = value.slice(offs, length);
            result.add(new KeyValue<>(evkey, evdata));
            offs += length;
        }
        return result;
    }

}
