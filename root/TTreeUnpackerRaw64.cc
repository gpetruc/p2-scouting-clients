#include "TTreeUnpackerRaw64.h"
#include <TTree.h>
#include "unpack.h"

unsigned long int TTreeUnpackerRaw64::unpack(const std::vector<std::string> &ins, const std::string &out) const {
  Data data;
  auto book = [=](TTree *tree, uint16_t &npuppi, uint64_t /*header*/, uint64_t payload[255], Data & /*d*/) {
    tree->Branch("nPuppi", &npuppi, "nPuppi/s");
    tree->Branch("Puppi_packed", payload, "Puppi_packed[nPuppi]/l");
  };

  auto decode = [](uint16_t & /*npuppi*/, uint64_t /*payload*/[255], Data & /*d*/) {};

  return unpackBase(ins, out, data, book, decode);
}