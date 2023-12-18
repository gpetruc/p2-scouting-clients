package p2scouting.kafka.serdes;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.kafka.common.serialization.Deserializer;

import p2scouting.core.PuppiSOAFloatRecord;

public class PuppiFloatEventDeserializer implements Deserializer<PuppiSOAFloatRecord> {

    @Override
    public PuppiSOAFloatRecord deserialize(String topic, byte[] data) {
        ByteBuffer buff = ByteBuffer.wrap(data);
        buff.order(ByteOrder.LITTLE_ENDIAN);
        short n = (short)buff.getShort();
        assert(data.length == 2 + 27*n);
        float[] pt = new float[n];
        float[] eta = new float[n];
        float[] phi = new float[n];
        float[] dxy = new float[n];
        float[] z0 = new float[n];
        float[] wpuppi = new float[n];
        short[] pdgid = new short[n];
        byte[] quality = new byte[n];
        buff.slice(2, 4*n).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(pt);
        buff.slice(2+4*n, 4*n).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(eta);
        buff.slice(2+8*n, 4*n).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(phi);
        buff.slice(2+12*n, 4*n).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(dxy);
        buff.slice(2+16*n, 4*n).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(z0);
        buff.slice(2+20*n, 4*n).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(wpuppi);
        buff.slice(2+24*n, 2*n).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().get(pdgid);
        buff.slice(2+26*n, n).get(quality);
        return new PuppiSOAFloatRecord(pt, eta, phi, pdgid, z0, dxy, wpuppi, quality);
    }
    
}
