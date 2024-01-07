package p2scouting.core;

public record PtEtaPhiMLorentzVector(float pt, float eta, float phi, float mass) {
    public final double px() { return pt * Math.cos(phi); }
    public final double py() { return pt * Math.sin(phi); }
    public final double pz() { return pt * Math.sinh(eta); }
    public final double p2() { 
        double sinh = Math.sinh(eta);
        return pt * pt * (1 + sinh*sinh); 
    }
    public final double m2() {
        return mass*mass;
    }
    public final double e2() {
        return p2() + m2();
    }
    public final double e() {
        return Math.sqrt(e2());
    }
    public XYZELorentzVector toXYZE() {
        return new XYZELorentzVector(px(), py(), pz(), e());
    }
    
}
