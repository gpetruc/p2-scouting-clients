#include "ArrowUnpackerFloats.h"
#include "../unpack.h"

ArrowUnpackerFloats::ArrowUnpackerFloats(unsigned int batchsize,
                                         ApacheUnpackMaker::Spec::FileKind fileKind,
                                         bool float16)
    : ArrowUnpackerBase(batchsize, fileKind),
      float16_(float16),
      floatType_(float16_ ? arrow::float16() : arrow::float32()),
      ptField_(arrow::field("pt", floatType_)),
      etaField_(arrow::field("eta", floatType_)),
      phiField_(arrow::field("phi", floatType_)),
      z0Field_(arrow::field("z0", floatType_)),
      dxyField_(arrow::field("dxy", floatType_)),
      wpuppiField_(arrow::field("wpuppi", floatType_)),
      pdgidField_(arrow::field("pdgId", arrow::int16())),
      qualityField_(arrow::field("quality", arrow::uint8())),
      puppiType_(arrow::struct_(
          {ptField_, etaField_, phiField_, z0Field_, dxyField_, wpuppiField_, pdgidField_, qualityField_})),
      puppisType_(arrow::list(puppiType_)),
      puppiField_(arrow::field("Puppi", puppisType_)),
      offsets_(1, 0) {
  schema_ = arrow::schema({runField_, orbitField_, bxField_, goodField_, puppiField_});
}

void ArrowUnpackerFloats::unpackAndCommitBatch() {
  // make tally
  offsets_.resize(1);
  for (auto n : nwords_)
    offsets_.emplace_back(offsets_.back() + n);
  unsigned int nallpuppi = offsets_.back();
  // unpack
  for (std::vector<float> *v : {&pt_, &eta_, &phi_, &dxy_, &z0_, &wpuppi_})
    v->resize(nallpuppi);
  pdgid_.resize(nallpuppi);
  quality_.resize(nallpuppi);
  for (unsigned int i = 0; i < nallpuppi; ++i) {
    readshared(data_[i], pt_[i], eta_[i], phi_[i]);
    if (readpid(data_[i], pdgid_[i])) {
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
  std::shared_ptr<arrow::Array> pt, eta, phi, z0, dxy, wpuppi;
  if (float16_) {
    pt = std::make_shared<arrow::HalfFloatArray>(nallpuppi, arrow::Buffer::Wrap(pt_));
    eta = std::make_shared<arrow::HalfFloatArray>(nallpuppi, arrow::Buffer::Wrap(eta_));
    phi = std::make_shared<arrow::HalfFloatArray>(nallpuppi, arrow::Buffer::Wrap(phi_));
    dxy = std::make_shared<arrow::HalfFloatArray>(nallpuppi, arrow::Buffer::Wrap(dxy_));
    z0 = std::make_shared<arrow::HalfFloatArray>(nallpuppi, arrow::Buffer::Wrap(z0_));
    wpuppi = std::make_shared<arrow::HalfFloatArray>(nallpuppi, arrow::Buffer::Wrap(wpuppi_));
  } else {
    pt = std::make_shared<arrow::FloatArray>(nallpuppi, arrow::Buffer::Wrap(pt_));
    eta = std::make_shared<arrow::FloatArray>(nallpuppi, arrow::Buffer::Wrap(eta_));
    phi = std::make_shared<arrow::FloatArray>(nallpuppi, arrow::Buffer::Wrap(phi_));
    dxy = std::make_shared<arrow::FloatArray>(nallpuppi, arrow::Buffer::Wrap(dxy_));
    z0 = std::make_shared<arrow::FloatArray>(nallpuppi, arrow::Buffer::Wrap(z0_));
    wpuppi = std::make_shared<arrow::FloatArray>(nallpuppi, arrow::Buffer::Wrap(wpuppi_));
  }
  std::shared_ptr<arrow::Array> pdgid(new arrow::Int16Array(nallpuppi, arrow::Buffer::Wrap(pdgid_)));
  std::shared_ptr<arrow::Array> quality(new arrow::UInt8Array(nallpuppi, arrow::Buffer::Wrap(quality_)));
  std::shared_ptr<arrow::Array> flatPuppi(
      new arrow::StructArray(puppiType_, nallpuppi, {pt, eta, phi, z0, dxy, wpuppi, pdgid, quality}));
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