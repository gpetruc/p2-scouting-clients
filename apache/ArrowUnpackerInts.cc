#include "ArrowUnpackerInts.h"
#include "../unpack.h"

ArrowUnpackerInts::ArrowUnpackerInts(unsigned int batchsize, ApacheUnpackMaker::Spec::FileKind fileKind)
    : ArrowUnpackerBase(batchsize, fileKind),
      ptField_(arrow::field("pt", arrow::uint16())),
      etaField_(arrow::field("eta", arrow::int16())),
      phiField_(arrow::field("phi", arrow::int16())),
      z0Field_(arrow::field("z0", arrow::int16())),
      dxyField_(arrow::field("dxy", arrow::int8())),
      wpuppiField_(arrow::field("wpuppi", arrow::uint16())),
      pidField_(arrow::field("pid", arrow::uint8())),
      qualityField_(arrow::field("quality", arrow::uint8())),
      puppiType_(arrow::struct_(
          {ptField_, etaField_, phiField_, z0Field_, dxyField_, wpuppiField_, pidField_, qualityField_})),
      puppisType_(arrow::list(puppiType_)),
      puppiField_(arrow::field("Puppi", puppisType_)),
      offsets_(1, 0) {
  schema_ = arrow::schema({runField_, orbitField_, bxField_, goodField_, puppiField_});
}

void ArrowUnpackerInts::unpackAndCommitBatch() {
  // make tally
  offsets_.resize(1);
  for (auto n : nwords_)
    offsets_.emplace_back(offsets_.back() + n);
  unsigned int nallpuppi = offsets_.back();
  // unpack
  for (std::vector<uint16_t> *v : {&pt_, &wpuppi_})
    v->resize(nallpuppi);
  for (std::vector<int16_t> *v : {&eta_, &phi_, &z0_})
    v->resize(nallpuppi);
  for (std::vector<uint8_t> *v : {&pid_, &quality_})
    v->resize(nallpuppi);
  dxy_.resize(nallpuppi);
  unpack_puppi_ints(nallpuppi,
                    data_.data(),
                    pt_.data(),
                    eta_.data(),
                    phi_.data(),
                    pid_.data(),
                    quality_.data(),
                    z0_.data(),
                    dxy_.data(),
                    wpuppi_.data());
  // commit
  std::shared_ptr<arrow::Array> run(new arrow::UInt16Array(entriesInBatch_, arrow::Buffer::Wrap(run_)));
  std::shared_ptr<arrow::Array> orbit(new arrow::UInt32Array(entriesInBatch_, arrow::Buffer::Wrap(orbit_)));
  std::shared_ptr<arrow::Array> bx(new arrow::UInt16Array(entriesInBatch_, arrow::Buffer::Wrap(bx_)));
  auto good = goodBuilder_->Finish();
  std::shared_ptr<arrow::Array> pt(new arrow::UInt16Array(nallpuppi, arrow::Buffer::Wrap(pt_)));
  std::shared_ptr<arrow::Array> eta(new arrow::Int16Array(nallpuppi, arrow::Buffer::Wrap(eta_)));
  std::shared_ptr<arrow::Array> phi(new arrow::Int16Array(nallpuppi, arrow::Buffer::Wrap(phi_)));
  std::shared_ptr<arrow::Array> dxy(new arrow::Int16Array(nallpuppi, arrow::Buffer::Wrap(dxy_)));
  std::shared_ptr<arrow::Array> z0(new arrow::Int8Array(nallpuppi, arrow::Buffer::Wrap(z0_)));
  std::shared_ptr<arrow::Array> wpuppi(new arrow::UInt16Array(nallpuppi, arrow::Buffer::Wrap(wpuppi_)));
  std::shared_ptr<arrow::Array> pid(new arrow::Int16Array(nallpuppi, arrow::Buffer::Wrap(pid_)));
  std::shared_ptr<arrow::Array> quality(new arrow::UInt8Array(nallpuppi, arrow::Buffer::Wrap(quality_)));
  std::shared_ptr<arrow::Array> flatPuppi(
      new arrow::StructArray(puppiType_, nallpuppi, {pt, eta, phi, z0, dxy, wpuppi, pid, quality}));
  std::shared_ptr<arrow::Array> puppi(
      new arrow::ListArray(puppisType_, entriesInBatch_, arrow::Buffer::Wrap(offsets_), flatPuppi));
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