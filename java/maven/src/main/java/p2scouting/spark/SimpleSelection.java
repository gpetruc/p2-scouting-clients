package p2scouting.spark;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

public final class SimpleSelection implements FilterFunction<Row> {
    private final float ptHigh;
    private final float ptMid;
    private final float ptLow;

    SimpleSelection(float ptHigh, float ptMid, float ptLow) {
        this.ptHigh = ptHigh;
        this.ptMid = ptMid;
        this.ptLow = ptLow;
    }

    @Override
    public final boolean call(Row row) throws Exception {
        float[] pts = row.getStruct(row.fieldIndex("puppis")).getAs("pt");
        int nlow = 0, nmid = 0, nhi = 0;
        for (float pt : pts) {
            if (pt >= ptHigh) nhi++;
            if (pt >= ptMid) nmid++;
            if (pt >= ptLow) nlow++;
        }
        return nhi >= 1 && nmid >= 2 && nlow >= 3;
    }

}
