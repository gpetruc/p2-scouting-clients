package p2scouting.core;

public record XYZELorentzVector(double px, double py, double pz, double e) {
    public final double pt() { return Math.hypot(px, py); }
    public final double phi() { return Math.atan2(py, px); }
    public final double eta() { 
        double pt = this.pt();
        if (pt == 0) {
            return pz > 0 ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
        }
        // there's no asinh
        double x = Math.abs(pz/pt);
        return Math.signum(pz)*Math.log(x + Math.sqrt(x*x + 1.0)); 
    }
    public final double p2() {
        return (px*px + py*py + pz*pz);
    }
    public final double m2() {
        return e*e - p2();
    }
    public final double mass() {
        return Math.sqrt(m2());
    }
    
}
