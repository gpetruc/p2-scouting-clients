package p2scouting.spark;

import java.util.Iterator;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class RawFileSplitter implements FlatMapFunction<Row, Row> {
    private StructType schema;
    private Encoder<Row> encoder;

    RawFileSplitter() {
        schema = new StructType()
                .add("header", DataTypes.LongType)
                .add("payload", DataTypes.createArrayType(DataTypes.LongType));
        encoder = Encoders.row(schema);
    }

    public StructType schema() {
        return schema;
    }

    public Encoder<Row> encoder() {
        return encoder;
    }

    private class UnpackingIterator implements Iterator<Row> {
        private final LongBuffer buff;

        UnpackingIterator(LongBuffer buff) {
            this.buff = buff;
            trimZeros();
        }

        @Override
        public boolean hasNext() {
            return buff.hasRemaining();
        }

        @Override
        public Row next() {
            long header = buff.get();
            int npuppi = (int) (header & 0xFFF);
            long[] evdata = new long[npuppi];
            buff.get(evdata);
            trimZeros();
            return RowFactory.create(header, evdata);
        }

        private final void trimZeros() {
            while (buff.hasRemaining()) {
                if (buff.get(buff.position()) == 0) {
                    buff.get();
                    continue;
                }
                break;
            }
        }
    }

    @Override
    public Iterator<Row> call(Row t) throws Exception {
        byte[] data = t.getAs("content");
        System.out.println("Found " + data.length + " bytes");
        ByteBuffer rawbuff = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());
        LongBuffer buff = rawbuff.asLongBuffer();
        return new UnpackingIterator(buff);
    }

}
