#include "ArrowUnpackerRaw64.h"
#include "../unpack.h"

ArrowUnpackerRaw64::ArrowUnpackerRaw64(unsigned int batchsize, ApacheUnpackMaker::Spec::FileKind fileKind)
    : ArrowUnpackerBase(batchsize, fileKind),
      puppisType_(arrow::list(arrow::uint64())),
      puppiField_(arrow::field("Puppi", puppisType_)),
      offsets_(1, 0) {
  schema_ = arrow::schema({runField_, orbitField_, bxField_, goodField_, puppiField_});
}

void ArrowUnpackerRaw64::unpackAndCommitBatch() {
  // make tally
  offsets_.resize(1);
  for (auto n : nwords_)
    offsets_.emplace_back(offsets_.back() + n);
  unsigned int nallpuppi = offsets_.back();
  // unpack
  std::shared_ptr<arrow::Array> run(new arrow::UInt16Array(entriesInBatch_, arrow::Buffer::Wrap(run_)));
  std::shared_ptr<arrow::Array> orbit(new arrow::UInt32Array(entriesInBatch_, arrow::Buffer::Wrap(orbit_)));
  std::shared_ptr<arrow::Array> bx(new arrow::UInt16Array(entriesInBatch_, arrow::Buffer::Wrap(bx_)));
  auto good = goodBuilder_->Finish();
  std::shared_ptr<arrow::Array> packed(new arrow::UInt64Array(nallpuppi, arrow::Buffer::Wrap(data_)));
  std::shared_ptr<arrow::Array> puppi(
      new arrow::ListArray(puppisType_, entriesInBatch_, arrow::Buffer::Wrap(offsets_), packed));
  if (outputFile_) {
    std::shared_ptr<arrow::RecordBatch> batch =
        arrow::RecordBatch::Make(schema_, entriesInBatch_, {run, orbit, bx, *good, puppi});
    writeRecordBatch(*batch);
  }
  entriesInBatch_ = 0;
  offsets_.resize(1);
  batches_++;
  goodBuilder_->Reset();
}