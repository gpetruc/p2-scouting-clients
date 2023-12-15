package p2scouting.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.Float4Writer;
import org.apache.arrow.vector.complex.writer.SmallIntWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import p2scouting.core.PuppiSOAFloatRecord;

/**
 * Helper class to write a PuppiSOAFloatRecord to an apache arrow IPC stream
 * file
 */

public class PuppiFloatsWriter {
    public PuppiFloatsWriter(BufferAllocator allocator, int batchSize) {
        this.allocator_ = allocator;
        var uint16 = new ArrowType.Int(16, false);
        var uint32 = new ArrowType.Int(32, false);
        runField_ = Field.notNullable("run", uint16);
        orbitField_ = Field.notNullable("orbit", uint32);
        bxField_ = Field.notNullable("bx", uint16);
        goodField_ = Field.notNullable("good", new ArrowType.Bool());
        var float32 = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        ptField_ = Field.notNullable("pt", float32);
        etaField_ = Field.notNullable("eta", float32);
        phiField_ = Field.notNullable("phi", float32);
        pdgidField_ = Field.notNullable("pdgId", new ArrowType.Int(16, true));
        var childFields = Arrays.asList(ptField_, etaField_, phiField_, pdgidField_);
        var structType = FieldType.notNullable(new ArrowType.Struct());
        var structField = new Field("item", structType, childFields);
        var listType = FieldType.notNullable(new ArrowType.List());
        puppiField_ = new Field("Puppi", listType, Arrays.asList(structField));
        schema_ = new Schema(Arrays.asList(runField_, orbitField_, bxField_, goodField_, puppiField_));
        System.out.println(schema_);
        root_ = VectorSchemaRoot.create(schema_, allocator_);
        run_ = (UInt2Vector) root_.getVector("run");
        orbit_ = (UInt4Vector) root_.getVector("orbit");
        bx_ = (UInt2Vector) root_.getVector("bx");
        good_ = (BitVector) root_.getVector("good");
        puppis_ = (ListVector) root_.getVector("Puppi");
        listWriter_ = puppis_.getWriter();
        structWriter_ = listWriter_.struct();
        ptWriter_ = structWriter_.float4(ptField_.getName());
        etaWriter_ = structWriter_.float4(etaField_.getName());
        phiWriter_ = structWriter_.float4(phiField_.getName());
        pdgidWriter_ = structWriter_.smallInt(pdgidField_.getName());
        batchSize_ = batchSize;
    }

    public void startFile(String outputFilename) throws IOException {
        fout_ = new FileOutputStream(outputFilename);
        writer_ = new ArrowStreamWriter(root_, /* DictionaryProvider= */null, fout_);
        writer_.start();
        index_ = -1;
    }

    public void startBatch() {
        index_ = 0;
        run_.allocateNew(batchSize_);
        orbit_.allocateNew(batchSize_);
        bx_.allocateNew(batchSize_);
        good_.allocateNew(batchSize_);
        puppis_.allocateNew();
    }

    public void fillEvent(short run, int orbit, short bx, boolean good, PuppiSOAFloatRecord puppis) throws IOException {
        if (index_ == -1)
            startBatch();
        run_.set(index_, run);
        orbit_.set(index_, orbit);
        bx_.set(index_, bx);
        good_.set(index_, good ? 1 : 0);
        listWriter_.setPosition(index_);
        listWriter_.startList();
        if (puppis != null) {
            for (int j = 0, n = puppis.n(); j < n; j++) {
                structWriter_.start();
                ptWriter_.writeFloat4(puppis.pt()[j]);
                etaWriter_.writeFloat4(puppis.eta()[j]);
                phiWriter_.writeFloat4(puppis.phi()[j]);
                pdgidWriter_.writeSmallInt(puppis.pdgid()[j]);
                structWriter_.end();
            }
            listWriter_.setValueCount(puppis.n());
        } else {
            listWriter_.setValueCount(0);
        }
        listWriter_.endList();
        index_++;
        if (index_ == batchSize_)
            doneBatch();
    }

    public void doneBatch() throws IOException {
        root_.setRowCount(index_);
        writer_.writeBatch();
        index_ = -1;
    }

    public void doneFile() throws IOException {
        if (index_ > 0)
            doneBatch();
        writer_.end();
        fout_.close();
    }

    protected BufferAllocator allocator_;

    protected Field runField_, orbitField_, bxField_, goodField_;
    protected Field ptField_, etaField_, phiField_, z0Field_, dxyField_, wpuppiField_;
    protected Field pdgidField_, qualityField_;
    protected Field puppiField_;

    protected Schema schema_;

    protected VectorSchemaRoot root_;

    protected UInt2Vector run_;
    protected UInt4Vector orbit_;
    protected UInt2Vector bx_;
    protected BitVector good_;
    protected ListVector puppis_;
    protected UnionListWriter listWriter_;
    protected StructWriter structWriter_;
    protected Float4Writer ptWriter_, etaWriter_, phiWriter_;
    protected SmallIntWriter pdgidWriter_;

    protected int batchSize_, index_;

    protected FileOutputStream fout_;
    protected ArrowStreamWriter writer_;

}
