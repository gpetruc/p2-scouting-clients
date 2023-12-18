package p2scouting.kafka.serdes;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.kafka.common.serialization.Deserializer;

import p2scouting.core.ScoutingEventHeaderRecord;

public class ScoutingEventHeaderDeserializer implements Deserializer<ScoutingEventHeaderRecord> {

    @Override
    public ScoutingEventHeaderRecord deserialize(String topic, byte[] data) {
        return ScoutingEventHeaderRecord.decode(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong());
    }
    
}
