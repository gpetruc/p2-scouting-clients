package p2scouting.core;

public record PuppiFloatRecord(float pt, float eta, float phi, short pdgid, float z0, float dxy, float wpuppi,
      byte quality) {
   public final double px() {
      return pt * Math.cos(phi);
   }

   public final double py() {
      return pt * Math.sin(phi);
   }

   public final double pz() {
      return pt * Math.sinh(eta);
   }

   public final float mass() {
      switch (Math.abs(pdgid)) {
         case 11: return 0.0005f;
         case 13: return 0.105f;
         case 211: return 0.1396f;
         case 130: return 0.5f;
         case 22: return 0.f;
         default: return 0.f;
      }
   }

   public final double e() {
      double sinh = Math.sinh(eta);
      float m = mass();
      return pt * pt * (1 + sinh * sinh) + m*m;
   }

   public XYZELorentzVector p4() {
      return new XYZELorentzVector(px(), py(), pz(), e());
   }
}
