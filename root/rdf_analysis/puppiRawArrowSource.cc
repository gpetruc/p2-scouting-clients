/**
 * RArrowDS2 source that unpacks Puppi RAW files on the fly 
 */

#include "puppiRawArrowSource.h"
#include "../../unpack.h"

#include <chrono>
#include <iostream>
#include <stdexcept>

PuppiRawArrowSource::PuppiRawArrowSource(const std::string &flavour, const std::vector<std::string> &fileNames)
    : flavour_(flavour),
      fileNames_(fileNames),
      batchSize_(3564),
      files_(fileNames_.size()),
      runField_(arrow::field("run", arrow::uint16())),
      orbitField_(arrow::field("orbit", arrow::uint32())),
      bxField_(arrow::field("bx", arrow::uint16())),
      goodField_(arrow::field("good", arrow::boolean())),
      run_(batchSize_),
      bx_(batchSize_),
      orbit_(batchSize_),
      goodBuilder_(std::make_shared<arrow::BooleanBuilder>()),
      nwords_(batchSize_),
      offsets_(1, 0) {
  if (flavour_ == "raw64") {
    puppiType_ = arrow::uint64();
    puppisType_ = arrow::list(arrow::uint64());
    puppisField_ = arrow::field("Puppi", puppisType_);
    schema_ = arrow::schema({runField_, orbitField_, bxField_, goodField_, puppisField_});
  } else {
    throw std::runtime_error("PuppiRawArrowSource:Unsupported format " + flavour_);
  }
  offsets_.reserve(batchSize_ + 1);
}

PuppiRawArrowSource::~PuppiRawArrowSource() {}

void PuppiRawArrowSource::Start() {
  for (unsigned int i = 0, n = fileNames_.size(); i < n; ++i) {
    files_[i].open(fileNames_[i], std::ios_base::in | std::ios_base::binary);
    if (!files_[i].good()) {
      throw std::runtime_error("Error opening " + fileNames_[i] + " for input");
    }
  }
  unpackTime_ = 0;
}
std::shared_ptr<arrow::RecordBatch> PuppiRawArrowSource::Next() {
  uint16_t run;
  uint32_t orbit;
  uint16_t bx;
  bool good;
  uint16_t nwords;
  uint64_t header;
  unsigned int entriesInBatch = 0;
  unsigned int nallwords = 0, capacity = std::max<unsigned>(data_.capacity(), batchSize_);
  data_.resize(capacity);
  offsets_.resize(1);
  for (int ifile = 0, nfiles = files_.size(); files_[ifile].good(); ifile = (ifile == nfiles - 1 ? 0 : ifile + 1)) {
    std::fstream &fin = files_[ifile];
    do {
      fin.read(reinterpret_cast<char *>(&header), sizeof(uint64_t));
    } while (header == 0 && fin.good());
    if (!header)
      continue;
    parseHeader(header, run, bx, orbit, good, nwords);
    run_[entriesInBatch] = run;
    orbit_[entriesInBatch] = orbit;
    bx_[entriesInBatch] = bx;
    (void)goodBuilder_->Append(good);
    if (nwords) {
      if (nallwords + nwords > capacity) {
        capacity = std::max<unsigned int>(nallwords + nwords, 2 * capacity);
        data_.resize(capacity);
      }
      fin.read(reinterpret_cast<char *>(&data_[nallwords]), nwords * sizeof(uint64_t));
      nallwords += nwords;
    }
    nwords_[entriesInBatch] = nwords;
    offsets_.push_back(offsets_.back() + nwords);
    entriesInBatch++;
    if ((ifile == nfiles - 1) && (entriesInBatch >= batchSize_))
      break;
  }
  if (entriesInBatch == 0)
    return std::shared_ptr<arrow::RecordBatch>();
  // unpack
  std::shared_ptr<arrow::Array> runArray(new arrow::UInt16Array(entriesInBatch, arrow::Buffer::Wrap(run_)));
  std::shared_ptr<arrow::Array> orbitArray(new arrow::UInt32Array(entriesInBatch, arrow::Buffer::Wrap(orbit_)));
  std::shared_ptr<arrow::Array> bxArray(new arrow::UInt16Array(entriesInBatch, arrow::Buffer::Wrap(bx_)));
  auto goodArray = *goodBuilder_->Finish();
  std::shared_ptr<arrow::Array> packedArray(new arrow::UInt64Array(nallwords, arrow::Buffer::Wrap(data_)));
  std::shared_ptr<arrow::Array> puppiArray(
      new arrow::ListArray(puppisType_, entriesInBatch, arrow::Buffer::Wrap(offsets_), packedArray));
  return arrow::RecordBatch::Make(schema_, entriesInBatch, {runArray, orbitArray, bxArray, goodArray, puppiArray});
  // fill record batch and return
}
