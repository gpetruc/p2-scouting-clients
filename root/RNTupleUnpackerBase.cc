#include "RNTupleUnpackerBase.h"
#include <filesystem>
#include <cassert>

void RNTupleUnpackerBase::setThreads(unsigned int threads) {
  ROOT::EnableImplicitMT(threads);
  printf("Enabled Implicit MT with %d threads\n", threads);
}

void RNTupleUnpackerBase::setCompression(const std::string &algo, unsigned int compressionLevel) {
  if (algo == "lzma")
    compression_ = ROOT::CompressionSettings(ROOT::kLZMA, compressionLevel);
  else if (algo == "zlib")
    compression_ = ROOT::CompressionSettings(ROOT::kZLIB, compressionLevel);
  else if (algo == "lz4")
    compression_ = ROOT::CompressionSettings(ROOT::kLZ4, compressionLevel);
  else if (algo == "zstd")
    compression_ = ROOT::CompressionSettings(ROOT::kZSTD, compressionLevel);
  else if (algo == "none") {
    compression_ = 0;
  } else
    throw std::runtime_error("Unsupported compression algo " + algo);
  if (compression_)
    printf("Enabled compression with %s, level %d\n", algo.c_str(), compressionLevel);
}

unsigned long int RNTupleUnpackerBase::closeOutput() {
  unsigned long int ret = 0;
  if (writer_) {
    writer_->CommitCluster();
    writer_.reset();
    ret = std::filesystem::file_size(fout_);
    fout_.clear();
  }
  return ret;
}

void RNTupleUnpackerBase::bookBase(const std::string &out, std::unique_ptr<ROOT::Experimental::RNTupleModel> model) {
  if (!out.empty()) {
    assert(fout_.empty() && !writer_);
    fout_ = out;
    if (out.length() <= 5 || out.substr(out.length() - 5) != ".root") {
      fout_ = out + ".root";
    }
    ROOT::Experimental::RNTupleWriteOptions options;
    options.SetCompression(compression_);
    writer_ = ROOT::Experimental::RNTupleWriter::Recreate(std::move(model), "Events", fout_.c_str(), options);
  }
}