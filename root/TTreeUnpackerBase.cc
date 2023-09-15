#include "TTreeUnpackerBase.h"

void TTreeUnpackerBase::setThreads(unsigned int threads) {
  ROOT::EnableImplicitMT(threads);
  printf("Enabled Implicit MT with %d threads\n", threads);
}

void TTreeUnpackerBase::setCompression(const std::string &algo, unsigned int level) {
  compressionLevel_ = level;
  if (algo == "lzma")
    compressionAlgo_ = ROOT::RCompressionSetting::EAlgorithm::kLZMA;
  else if (algo == "zlib")
    compressionAlgo_ = ROOT::RCompressionSetting::EAlgorithm::kZLIB;
  else if (algo == "lz4")
    compressionAlgo_ = ROOT::RCompressionSetting::EAlgorithm::kLZ4;
  else if (algo == "zstd")
    compressionAlgo_ = ROOT::RCompressionSetting::EAlgorithm::kZSTD;
  else if (algo == "none") {
    compressionLevel_ = 0;
    compressionAlgo_ = ROOT::RCompressionSetting::EAlgorithm::kZLIB;
  } else
    throw std::runtime_error("Unsupported compression algo " + algo);
  if (level)
    printf("Enabled compression with %s, level %d\n", algo.c_str(), level);
}
