package p2scouting.kafka;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import p2scouting.core.PuppiSOAFloatRecord;
import p2scouting.core.PuppiUnpacker;
import p2scouting.core.ScoutingEventHeaderRecord;

public class PuppiEventUnpackerKernel implements KeyValueMapper<Long, ByteBuffer, KeyValue<ScoutingEventHeaderRecord, PuppiSOAFloatRecord>> {
    @Override
    public KeyValue<ScoutingEventHeaderRecord, PuppiSOAFloatRecord> apply(Long key, ByteBuffer value) {
       LongBuffer buff = value.order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
       long header = buff.get(0);
       assert((header >>> 12) == key);
       var eh = ScoutingEventHeaderRecord.decode(key << 12);
       var puppis = PuppiUnpacker.unpackMany(buff.slice(1,buff.limit()-1));
       if (eh.bx() < 25) {
         System.out.printf("Unpacking %013x run %d, orbit %d, bx %d, npuppi %d\n", key, eh.run(), eh.orbit(), eh.bx(), puppis.n());
       }
       return new KeyValue<>(eh, puppis);
    }
}
