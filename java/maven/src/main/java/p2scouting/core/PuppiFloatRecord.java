package p2scouting.core;

public record PuppiFloatRecord(float pt, float eta, float phi, short pdgid, float z0, float dxy, float wpuppi, byte quality) {
   public final double px() { return pt * Math.cos(phi); }
   public final double py() { return pt * Math.sin(phi); }
   public final double pz() { return pt * Math.sinh(eta); }
}
