package p2scouting.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import p2scouting.core.PuppiSOAFloatRecord;
import p2scouting.core.PuppiUnpacker;

public class EventUnpacker implements MapFunction<Row, Row> {
    private StructType puppisType;
    private StructType schema;
    private Encoder<Row> encoder;

    EventUnpacker() {
        puppisType = new StructType()
                .add("pt", DataTypes.createArrayType(DataTypes.FloatType))
                .add("eta", DataTypes.createArrayType(DataTypes.FloatType));
        schema = new StructType()
                .add("run", DataTypes.ShortType)
                .add("orbit", DataTypes.IntegerType)
                .add("bx", DataTypes.ShortType)
                .add("good", DataTypes.BooleanType)
                .add("puppis", puppisType);
        encoder = Encoders.row(schema);
    }

    public StructType schema() {
        return schema;
    }

    public Encoder<Row> encoder() {
        return encoder;
    }

    @Override
    public Row call(Row t) throws Exception {
        Long header = t.getLong(0);
        long[] data = t.getAs(1);
        short run = (short) ((header >> 54) & 0x3F);
        int orbit = (int) ((header >>> 24) & 0xFFFF_FFFF);
        short bx = (short) ((header >> 12) & 0xFFF);
        boolean good = (header & 0x2000_0000_0000_0000l) == 0;
        PuppiSOAFloatRecord soa = PuppiUnpacker.unpackMany(data);
        if (orbit == 1 && bx < 30)
            System.out.printf("EU %016x, orbit %d, bx %d, n %d, data = [%016x, ...], pts = [ %.2f, ... ]\n", header,
                    orbit, bx, soa.n(), (soa.n() > 0 ? data[0] : 0l), (soa.n() > 0 ? soa.pt()[0] : 0.f));
        return RowFactory.create(run, orbit, bx, good, RowFactory.create(soa.pt(), soa.eta()));
    }

}
