#include "IPCUnpackerBase.h"
#include <cassert>
#include <stdexcept>
#include <filesystem>
#include <fstream>
#include <chrono>
#include "../unpack.h"

IPCUnpackerBase::IPCUnpackerBase(unsigned int batchsize)
    : batchsize_(batchsize),
      compressionMethod_("none"),
      compressionLevel_(),
      runField_(arrow::field("run", arrow::uint16())),
      orbitField_(arrow::field("orbit", arrow::uint32())),
      bxField_(arrow::field("bx", arrow::uint16())),
      goodField_(arrow::field("good", arrow::boolean())),
      run_(batchsize),
      bx_(batchsize),
      orbit_(batchsize),
      goodBuilder_(std::make_shared<arrow::BooleanBuilder>()),
      nwords_(batchsize) {}

void IPCUnpackerBase::setThreads(unsigned int threads) { arrow::SetCpuThreadPoolCapacity(threads); }

void IPCUnpackerBase::bookOutput(const std::string &out) {
  std::shared_ptr<arrow::io::FileOutputStream> output_file;
  if (!out.empty()) {
    assert(fout_.empty());
    fout_ = out;
    if (out.length() <= 7 || out.substr(out.length() - 6) != ".arrow") {
      fout_ = out + ".arrow";
    }
    outputFile_ = *arrow::io::FileOutputStream::Open(fout_);
    arrow::ipc::IpcWriteOptions ipcWriteOptions = arrow::ipc::IpcWriteOptions::Defaults();
    if (compressionMethod_ == "lz4") {
      ipcWriteOptions.codec = *arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME, compressionLevel_);
    } else if (compressionMethod_ == "zstd") {
      ipcWriteOptions.codec = *arrow::util::Codec::Create(arrow::Compression::ZSTD, compressionLevel_);
    } else if (compressionMethod_ != "none") {
      throw std::invalid_argument("Unknown compression " + compressionMethod_);
    }
    batchWriter_ = *arrow::ipc::MakeStreamWriter(outputFile_, schema_, ipcWriteOptions);
    entriesInBatch_ = 0;
    batches_ = 0;
  }
}

unsigned long int IPCUnpackerBase::closeOutput() {
  unsigned long int ret = 0;
  if (entriesInBatch_)
    unpackAndCommitBatch();
  if (outputFile_) {
    outputFile_->Close();
    ret = std::filesystem::file_size(fout_);
    fout_.clear();
  }
  return ret;
}

UnpackerBase::Report IPCUnpackerBase::unpackFiles(const std::vector<std::string> &ins, const std::string &out) {
  bookOutput(out);
  Report ret(0);
  unsigned long int bytes_in = 0;

  auto tstart = std::chrono::steady_clock::now();
  std::vector<std::fstream> fins;
  for (auto &in : ins) {
    fins.emplace_back(in, std::ios_base::in | std::ios_base::binary);
    if (!fins.back().good()) {
      throw std::runtime_error("Error opening " + in + " for otput");
    }
    bytes_in += std::filesystem::file_size(in);
  }
  ret.bytes_in = bytes_in;

  uint16_t run;
  uint32_t orbit;
  uint16_t bx;
  bool good;
  uint16_t nwords;
  uint64_t header;
  unsigned int nallwords = 0, capacity = std::max<unsigned>(data_.capacity(), batchsize_);
  data_.resize(capacity);
  // loop
  for (int ifile = 0, nfiles = fins.size(); fins[ifile].good(); ifile = (ifile == nfiles - 1 ? 0 : ifile + 1)) {
    std::fstream &fin = fins[ifile];
    do {
      fin.read(reinterpret_cast<char *>(&header), sizeof(uint64_t));
    } while (header == 0 && fin.good());
    if (!header)
      continue;
    parseHeader(header, run, bx, orbit, good, nwords);
    fillBase(run, orbit, bx, good);
    if (nwords) {
      if (nallwords + nwords > capacity) {
        capacity = std::max<unsigned int>(nallwords + nwords, 2 * capacity);
        data_.resize(capacity);
      }
      fin.read(reinterpret_cast<char *>(&data_[nallwords]), nwords * sizeof(uint64_t));
      nallwords += nwords;
    }
    nwords_[entriesInBatch_] = nwords;
    ret.entries++;
    entriesInBatch_++;
    if (entriesInBatch_ == batchsize_) {
      unpackAndCommitBatch();
      nallwords = 0;
    }
  }

  ret.bytes_out = closeOutput();
  ret.time = (std::chrono::duration<double>(std::chrono::steady_clock::now() - tstart)).count();
  return ret;
}