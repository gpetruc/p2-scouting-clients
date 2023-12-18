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
       return new KeyValue<>(ScoutingEventHeaderRecord.decode(key), PuppiUnpacker.unpackMany(buff));
    }
}
