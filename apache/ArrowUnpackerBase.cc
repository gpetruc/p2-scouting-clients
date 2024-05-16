#include "ArrowUnpackerBase.h"
#include <cassert>
#include <stdexcept>
#include <filesystem>
#include <fstream>
#include <chrono>
#include "../unpack.h"

ArrowUnpackerBase::ArrowUnpackerBase(unsigned int batchsize, ApacheUnpackMaker::Spec::FileKind fileKind)
    : batchsize_(batchsize),
      fileKind_(fileKind),
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

void ArrowUnpackerBase::setThreads(unsigned int threads) { arrow::SetCpuThreadPoolCapacity(threads); }

void ArrowUnpackerBase::bookOutput(const std::string &out) {
  if (!out.empty()) {
    assert(fout_.empty());
    fout_ = out;
    if (fileKind_ == ApacheUnpackMaker::Spec::FileKind::IPCStream ||
        fileKind_ == ApacheUnpackMaker::Spec::FileKind::IPCFile) {
      if (out.length() <= 7 || out.substr(out.length() - 6) != ".arrow") {
        fout_ = out + ".arrow";
      }
#ifdef USE_PARQUET
    } else if (fileKind_ == ApacheUnpackMaker::Spec::FileKind::Parquet) {
      if (out.length() <= 9 || out.substr(out.length() - 8) != ".parquet") {
        fout_ = out + ".parquet";
      }
#endif
    }
    outputFile_ = *arrow::io::FileOutputStream::Open(fout_);
    if (fileKind_ == ApacheUnpackMaker::Spec::FileKind::IPCStream ||
        fileKind_ == ApacheUnpackMaker::Spec::FileKind::IPCFile) {
      arrow::ipc::IpcWriteOptions ipcWriteOptions = arrow::ipc::IpcWriteOptions::Defaults();
      if (compressionMethod_ == "lz4") {
        ipcWriteOptions.codec = *arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME, compressionLevel_);
      } else if (compressionMethod_ == "zstd") {
        ipcWriteOptions.codec = *arrow::util::Codec::Create(arrow::Compression::ZSTD, compressionLevel_);
      } else if (compressionMethod_ != "none") {
        throw std::invalid_argument("Unknown compression " + compressionMethod_);
      }
      if (fileKind_ == ApacheUnpackMaker::Spec::FileKind::IPCStream) {
        batchWriter_ = *arrow::ipc::MakeStreamWriter(outputFile_, schema_, ipcWriteOptions);
      } else if (fileKind_ == ApacheUnpackMaker::Spec::FileKind::IPCFile) {
        batchWriter_ = *arrow::ipc::MakeFileWriter(outputFile_, schema_, ipcWriteOptions);
      }
    }
#ifdef USE_PARQUET
  } else if (fileKind_ == ApacheUnpackMaker::Spec::FileKind::Parquet) {
    // Choose compression
    std::shared_ptr<parquet::WriterProperties> props;
    auto builder = parquet::WriterProperties::Builder();
    arrow::ipc::IpcWriteOptions ipcWriteOptions = arrow::ipc::IpcWriteOptions::Defaults();
    if (compressionMethod_ == "lz4") {
      props = builder.compression(arrow::Compression::LZ4_HADOOP)->compression_level(compressionLevel_)->build();
    } else if (compressionMethod_ == "zlib") {
      props = builder.compression(arrow::Compression::GZIP)->compression_level(compressionLevel_)->build();
    } else if (compressionMethod_ == "zstd") {
      props = builder.compression(arrow::Compression::ZSTD)->compression_level(compressionLevel_)->build();
    } else if (compressionMethod_ == "snappy") {
      props = builder.compression(arrow::Compression::SNAPPY)->compression_level(compressionLevel_)->build();
    } else if (compressionMethod_ != "none") {
      props = builder.build();
    } else if (compressionMethod_ != "none") {
      throw std::invalid_argument("Unknown compression " + compressionMethod_);
    }
    //std::shared_ptr<parquet::ArrowWriterProperties> arrow_props =
    //    parquet::ArrowWriterProperties::Builder().store_schema()->build();
    std::shared_ptr<parquet::ArrowWriterProperties> arrow_props = parquet::ArrowWriterProperties::Builder().build();
    parquetWriter_ =
        *parquet::arrow::FileWriter::Open(*schema_, arrow::default_memory_pool(), outputFile_, props, arrow_props);
#endif
  }
  entriesInBatch_ = 0;
  batches_ = 0;
}

unsigned long int ArrowUnpackerBase::closeOutput() {
  unsigned long int ret = 0;
  if (entriesInBatch_)
    unpackAndCommitBatch();
  if (outputFile_) {
    if (batchWriter_)
      batchWriter_->Close();
    outputFile_->Close();
    ret = std::filesystem::file_size(fout_);
    fout_.clear();
  }
  return ret;
}

UnpackerBase::Report ArrowUnpackerBase::myUnpackFiles(const std::vector<std::string> &ins, const std::string &out) {
  bookOutput(out);

  Report ret(0);
  unsigned long int bytes_in = 0;

  auto tstart = std::chrono::steady_clock::now();
  std::vector<std::fstream> fins;
  for (std::string in : ins) {
    fins.emplace_back(in, std::ios_base::in | std::ios_base::binary);
    if (!fins.back().good()) {
      throw std::runtime_error("Error opening " + in + " for input");
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
    if (!header || !fin.good())
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