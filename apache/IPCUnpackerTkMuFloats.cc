#include "IPCUnpackerTkMuFloats.h"
#include "../unpack.h"

IPCUnpackerTkMuFloats::IPCUnpackerTkMuFloats(unsigned int batchsize, bool float16)
    : IPCUnpackerBase(batchsize),
      float16_(float16),
      floatType_(float16_ ? arrow::float16() : arrow::float32()),
      ptField_(arrow::field("pt", floatType_)),
      etaField_(arrow::field("eta", floatType_)),
      phiField_(arrow::field("phi", floatType_)),
      z0Field_(arrow::field("z0", floatType_)),
      d0Field_(arrow::field("d0", floatType_)),
      betaField_(arrow::field("beta", floatType_)),
      chargeField_(arrow::field("charge", arrow::int8())),
      qualityField_(arrow::field("quality", arrow::uint8())),
      isolationField_(arrow::field("isolation", arrow::uint8())),
      tkmuType_(arrow::struct_(
          {ptField_, etaField_, phiField_, chargeField_, z0Field_, d0Field_, qualityField_, isolationField_, betaField_})),
      tkmusType_(arrow::list(tkmuType_)),
      tkmuField_(arrow::field("TkMu", tkmusType_)),
      offsets_(1, 0) {
  schema_ = arrow::schema({runField_, orbitField_, bxField_, goodField_, tkmuField_});
}

void IPCUnpackerTkMuFloats::unpackAndCommitBatch() {
  // make tally
  offsets_.resize(1);
  for (auto n : nwords_) {
    int nmu = (n*2)/3;
    offsets_.emplace_back(offsets_.back() + nmu);
  }
  unsigned int nalltkmu = offsets_.back();
  // unpack
  for (std::vector<float> *v : {&pt_, &eta_, &phi_, &d0_, &z0_, &beta_})
    v->resize(nalltkmu);
  charge_.resize(nalltkmu);
  isolation_.resize(nalltkmu);
  quality_.resize(nalltkmu);
  unsigned int imu = 0, iword = 0;
  for (auto n : nwords_) {
      uint16_t nmu;
      decode_gmt_tkmu(n,
                  &data_[iword],
                  nmu,
                  &pt_[imu],
                  &eta_[imu],
                  &phi_[imu],
                  &charge_[imu],
                  &z0_[imu],
                  &d0_[imu],
                  &quality_[imu],
                  &isolation_[imu],
                  &beta_[imu]);
    iword += n;
    imu += nmu;
  }
  assert(imu == nalltkmu);
  // commit
  std::shared_ptr<arrow::Array> run(new arrow::UInt16Array(entriesInBatch_, arrow::Buffer::Wrap(run_)));
  std::shared_ptr<arrow::Array> orbit(new arrow::UInt32Array(entriesInBatch_, arrow::Buffer::Wrap(orbit_)));
  std::shared_ptr<arrow::Array> bx(new arrow::UInt16Array(entriesInBatch_, arrow::Buffer::Wrap(bx_)));
  auto good = goodBuilder_->Finish();
  std::shared_ptr<arrow::Array> pt, eta, phi, z0, d0, beta;
  if (float16_) {
    pt = std::make_shared<arrow::HalfFloatArray>(nalltkmu, arrow::Buffer::Wrap(pt_));
    eta = std::make_shared<arrow::HalfFloatArray>(nalltkmu, arrow::Buffer::Wrap(eta_));
    phi = std::make_shared<arrow::HalfFloatArray>(nalltkmu, arrow::Buffer::Wrap(phi_));
    d0 = std::make_shared<arrow::HalfFloatArray>(nalltkmu, arrow::Buffer::Wrap(d0_));
    z0 = std::make_shared<arrow::HalfFloatArray>(nalltkmu, arrow::Buffer::Wrap(z0_));
    beta = std::make_shared<arrow::HalfFloatArray>(nalltkmu, arrow::Buffer::Wrap(beta_));
  } else {
    pt = std::make_shared<arrow::FloatArray>(nalltkmu, arrow::Buffer::Wrap(pt_));
    eta = std::make_shared<arrow::FloatArray>(nalltkmu, arrow::Buffer::Wrap(eta_));
    phi = std::make_shared<arrow::FloatArray>(nalltkmu, arrow::Buffer::Wrap(phi_));
    d0 = std::make_shared<arrow::FloatArray>(nalltkmu, arrow::Buffer::Wrap(d0_));
    z0 = std::make_shared<arrow::FloatArray>(nalltkmu, arrow::Buffer::Wrap(z0_));
    beta = std::make_shared<arrow::FloatArray>(nalltkmu, arrow::Buffer::Wrap(beta_));
  }
  std::shared_ptr<arrow::Array> charge(new arrow::Int16Array(nalltkmu, arrow::Buffer::Wrap(charge_)));
  std::shared_ptr<arrow::Array> quality(new arrow::UInt8Array(nalltkmu, arrow::Buffer::Wrap(quality_)));
  std::shared_ptr<arrow::Array> isolation(new arrow::UInt8Array(nalltkmu, arrow::Buffer::Wrap(isolation_)));
  std::shared_ptr<arrow::Array> flatPuppi(
      new arrow::StructArray(tkmuType_, nalltkmu, {pt, eta, phi, charge, z0, d0, quality, isolation, beta}));
  std::shared_ptr<arrow::Array> tkmu(
      new arrow::ListArray(tkmusType_, entriesInBatch_, arrow::Buffer::Wrap(offsets_), flatPuppi));
  if (outputFile_) {
    std::shared_ptr<arrow::RecordBatch> batch =
        arrow::RecordBatch::Make(schema_, entriesInBatch_, {run, orbit, bx, *good, tkmu});
    batchWriter_->WriteRecordBatch(*batch);
  }
  entriesInBatch_ = 0;
  offsets_.resize(1);
  batches_++;
  goodBuilder_->Reset();
}