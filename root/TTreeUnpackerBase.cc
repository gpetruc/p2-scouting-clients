#include "TTreeUnpackerBase.h"
#include <filesystem>

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

void TTreeUnpackerBase::bookOutputBase(const std::string &out) {
  if (!out.empty()) {
    assert(file_ == nullptr && tree_ == nullptr);
    fout_ = out;
    if (out.length() <= 5 || out.substr(out.length() - 5) != ".root") {
      fout_ = out + ".root";
    }
    file_ = TFile::Open(fout_.c_str(), "RECREATE", "", compressionLevel_);
    if (file_ == nullptr || !file_) {
      throw std::runtime_error("Error opening " + fout_ + " for output");
    }
    if (compressionLevel_)
      file_->SetCompressionAlgorithm(compressionAlgo_);
    tree_ = new TTree("Events", "Events");
    tree_->Branch("run", &run_, "run/s");
    tree_->Branch("orbit", &orbit_, "orbit/i");
    tree_->Branch("bx", &bx_, "bx/s");
    tree_->Branch("good", &good_, "good/O");
    tree_->Branch("nPuppi", &npuppi_, "nPuppi/s");
  }
}
unsigned long int TTreeUnpackerBase::closeOutput() {
  unsigned long int ret = 0;
  if (tree_) {
    tree_->Write();
    file_->Close();
    tree_ = nullptr;
    file_ = nullptr;
    ret = std::filesystem::file_size(fout_);
    fout_.clear();
  }
  return ret;
}
