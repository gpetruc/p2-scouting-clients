#include "IPCUnpackerInts.h"
#include "../unpack.h"

IPCUnpackerInts::IPCUnpackerInts(unsigned int batchsize)
    : IPCUnpackerBase(batchsize),
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

void IPCUnpackerInts::unpackAndCommitBatch() {
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
  for (unsigned int i = 0; i < nallpuppi; ++i) {
    readshared(data_[i], pt_[i], eta_[i], phi_[i]);
    pid_[i] = (data_[i] >> 37) & 0x7;
    if (pid_[i] > 1) {
      readcharged(data_[i], z0_[i], dxy_[i], quality_[i]);
      wpuppi_[i] = 0;
    } else {
      readneutral(data_[i], wpuppi_[i], quality_[i]);
      z0_[i] = 0;
      dxy_[i] = 0;
    }
  }
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
    batchWriter_->WriteRecordBatch(*batch);
  }
  entriesInBatch_ = 0;
  offsets_.resize(1);
  batches_++;
  goodBuilder_->Reset();
}