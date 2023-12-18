package p2scouting.kafka.serdes;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.kafka.common.serialization.Serializer;

import p2scouting.core.PuppiSOAFloatRecord;

public class PuppiFloatEventSerializer implements Serializer<PuppiSOAFloatRecord> {

    @Override
    public byte[] serialize(String topic, PuppiSOAFloatRecord data) {
        short n = (short)data.n();
        byte[] ret = new byte[2+n*27];
        ByteBuffer buff = ByteBuffer.wrap(ret);
        buff.order(ByteOrder.LITTLE_ENDIAN);
        buff.putShort(n);
        buff.slice(2, 4*n).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(data.pt());
        buff.slice(2+4*n, 4*n).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(data.eta());
        buff.slice(2+8*n, 4*n).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(data.phi());
        buff.slice(2+12*n, 4*n).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(data.dxy());
        buff.slice(2+16*n, 4*n).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(data.z0());
        buff.slice(2+20*n, 4*n).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(data.wpuppi());
        buff.slice(2+24*n, 2*n).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().put(data.pdgid());
        buff.slice(2+26*n, n).put(data.quality());
        return ret;
    }
    
}
