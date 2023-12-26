package p2scouting.kafka.serdes;

import java.nio.ByteBuffer;

import org.apache.kafka.common.serialization.Serializer;

import p2scouting.core.ScoutingEventHeaderRecord;

public class ScoutingEventHeaderSerializer implements Serializer<ScoutingEventHeaderRecord> {

    @Override
    public byte[] serialize(String topic, ScoutingEventHeaderRecord data) {
        byte[] ret = new byte[8];
        ByteBuffer buff = ByteBuffer.wrap(ret);
        buff.putLong(data.encode());
        return ret;
    }
    
}
