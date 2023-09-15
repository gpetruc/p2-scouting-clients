#include "RNTupleUnpackerRaw64.h"
#include "unpack.h"

unsigned long int RNTupleUnpackerRaw64::unpack(const std::vector<std::string> &ins, const std::string &out) const {
  std::vector<std::fstream> fins;
  for (auto &in : ins) {
    fins.emplace_back(in, std::ios_base::in | std::ios_base::binary);
    if (!fins.back().good()) {
      throw std::runtime_error("Error opening " + in + " for otput");
    }
  }

  auto model = ROOT::Experimental::RNTupleModel::Create();
  auto p_run = model->MakeField<uint16_t>("run");
  auto p_orbit = model->MakeField<uint32_t>("orbit");
  auto p_bx = model->MakeField<uint16_t>("bx");
  auto p_good = model->MakeField<bool>("good");
  auto p_puppi = model->MakeField<std::vector<uint64_t>>("Puppi_packed");

  std::unique_ptr<ROOT::Experimental::RNTupleWriter> writer;
  if (!out.empty()) {
    printf("Writing to %s\n", out.c_str());
    ROOT::Experimental::RNTupleWriteOptions options;
    options.SetCompression(compression_);
    writer = ROOT::Experimental::RNTupleWriter::Recreate(std::move(model), "Events", out.c_str(), options);
  }

  uint16_t npuppi;
  uint64_t header;

  // loop
  unsigned long int entries = 0;
  for (int ifile = 0, nfiles = fins.size(); fins[ifile].good(); ifile = (ifile == nfiles - 1 ? 0 : ifile + 1)) {
    std::fstream &fin = fins[ifile];
    do {
      fin.read(reinterpret_cast<char *>(&header), sizeof(uint64_t));
    } while (header == 0 && fin.good());
    if (!header)
      continue;
    parseHeader(header, *p_run, *p_bx, *p_orbit, *p_good, npuppi);
    p_puppi->resize(npuppi);
    if (npuppi) {
      fin.read(reinterpret_cast<char *>(&p_puppi->front()), npuppi * sizeof(uint64_t));
    }
    if (writer) {
      writer->Fill();
    }
    entries++;
  }
  // close
  return entries;
}