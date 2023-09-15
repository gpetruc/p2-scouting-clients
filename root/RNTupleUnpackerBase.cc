#include "RNTupleUnpackerBase.h"

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
