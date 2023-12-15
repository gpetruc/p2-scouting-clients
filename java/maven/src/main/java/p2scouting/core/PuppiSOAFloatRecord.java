package p2scouting.core;

public record PuppiSOAFloatRecord(float[] pt, float[] eta, float[] phi, short[] pdgid, float[] z0, float[] dxy, float[] wpuppi,
        byte[] quality) {
    public PuppiSOAFloatRecord {
        if (eta.length != pt.length ||
                phi.length != pt.length ||
                pdgid.length != pt.length ||
                z0.length != pt.length ||
                dxy.length != pt.length ||
                wpuppi.length != pt.length ||
                quality.length != pt.length)
            throw new IllegalArgumentException("Mismatch in size between the input arrays");

    }

    public final int n() {
        return pt.length;
    }

    public final double px(int i) {
        return pt[i] * Math.cos(phi[i]);
    }

    public final double py(int i) {
        return pt[i] * Math.sin(phi[i]);
    }

    public final double pz(int i) {
        return pt[i] * Math.sinh(eta[i]);
    }
}
