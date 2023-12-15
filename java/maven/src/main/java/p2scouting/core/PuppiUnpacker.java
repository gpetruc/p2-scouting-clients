package p2scouting.core;

import java.nio.LongBuffer;

public class PuppiUnpacker {
    public static final float ETAPHI_LSB = (float) (Math.PI / 720);

    public final static short intPt(long data) {
        return (short) (data & 0x3FFF);
    }

    public final static short intEta(long data) {
        return (short) ((((data >> 25) & 1) != 0) ? ((data >> 14) | (-0x800)) : ((data >> 14) & (0xFFF)));
    }

    public final static short intPhi(long data) {
        return (short) ((((data >> 36) & 1) != 0) ? ((data >> 26) | (-0x400)) : ((data >> 26) & (0x7FF)));
    }

    public final static byte pid(long data) {
        return (byte) ((data >> 37) & 0x7);
    }

    public final static byte quality(long data, int pid) {
        return (byte) ((pid > 1) ? ((data >> 58) & 0x7) : ((data >> 50) & 0x3F));
    }

    public static short pdgId(long data) {
        return pdgId(pid(data));
    }

    public static short pdgId(byte pid) {
        final short[] PDGIDS = { 130, 22, -211, 211, 11, -11, 13, -13 };
        return PDGIDS[pid];
    }

    public final static float floatPt(long data) {
        return intPt(data) * 0.25f;
    }

    public final static float floatEta(long data) {
        return intEta(data) * ETAPHI_LSB;
    }

    public static float floatPhi(long data) {
        return intPhi(data) * ETAPHI_LSB;
    }

    public final static PuppiFloatRecord unpackOne(long packed) {
        var id = pid(packed);
        return new PuppiFloatRecord(floatPt(packed), floatEta(packed), floatPhi(packed), pdgId(id), 0.f, 0.f, 0.f,
                quality(packed, id));
    }
    public final static PuppiSOAFloatRecord unpackMany(long[] packed) {
        int n = packed.length;
        float[] pt = new float[n];
        float[] eta = new float[n];
        float[] phi = new float[n];
        short[] pdgId = new short[n];
        float[] z0 = new float[n];
        float[] dxy = new float[n];
        float[] wpuppi = new float[n];
        byte[] quality = new byte[n];
        for (int i = 0; i < n; ++i) {
            var id = pid(packed[i]);
            pt[i] = floatPt(packed[i]);
            eta[i] = floatEta(packed[i]);
            phi[i] = floatPhi(packed[i]);
            pdgId[i] = pdgId(id);
            // ...
        }
        return new PuppiSOAFloatRecord(pt, eta, phi, pdgId, z0, dxy, wpuppi, quality);
    }
    public final static PuppiSOAFloatRecord unpackMany(LongBuffer buffer) {
        int n = buffer.limit();
        float[] pt = new float[n];
        float[] eta = new float[n];
        float[] phi = new float[n];
        short[] pdgId = new short[n];
        float[] z0 = new float[n];
        float[] dxy = new float[n];
        float[] wpuppi = new float[n];
        byte[] quality = new byte[n];
        for (int i = 0; i < n; ++i) {
            long packed = buffer.get(i);
            var id = pid(packed);
            pt[i] = floatPt(packed);
            eta[i] = floatEta(packed);
            phi[i] = floatPhi(packed);
            pdgId[i] = pdgId(id);
            // ...
        }
        return new PuppiSOAFloatRecord(pt, eta, phi, pdgId, z0, dxy, wpuppi, quality);
    }
}
