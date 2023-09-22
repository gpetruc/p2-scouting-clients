#ifndef p2_clients_RNTupleUnpackerBase_h
#define p2_clients_RNTupleUnpackerBase_h
#include <chrono>
#include <TROOT.h>
#include <TFile.h>
#include <ROOT/RNTuple.hxx>
#include <ROOT/RNTupleModel.hxx>
#include <ROOT/RNTupleUtil.hxx>
#include "../UnpackerBase.h"
#include "../unpack.h"

class RNTupleUnpackerBase : public UnpackerBase {
public:
  RNTupleUnpackerBase() : compression_(0) {}
  ~RNTupleUnpackerBase() override {}

  Report unpack(const std::vector<std::string> &ins, const std::string &out) const override = 0;
  void setThreads(unsigned int threads) override;
  void setCompression(const std::string &algo, unsigned int level) override;

protected:
  int compression_;

  template <typename T, typename B, typename D>
  Report unpackBase(
      const std::vector<std::string> &ins, const std::string &out, T &data, B makeModel, D fillModel) const {
    auto tstart = std::chrono::steady_clock::now();
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
    makeModel(*model, data);

    std::unique_ptr<ROOT::Experimental::RNTupleWriter> writer;
    if (!out.empty()) {
      ROOT::Experimental::RNTupleWriteOptions options;
      options.SetCompression(compression_);
      writer = ROOT::Experimental::RNTupleWriter::Recreate(std::move(model), "Events", out.c_str(), options);
    }

    uint16_t npuppi;
    uint64_t header, payload[255];

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
      if (npuppi)
        fin.read(reinterpret_cast<char *>(&payload[0]), npuppi * sizeof(uint64_t));
      if (writer) {
        fillModel(npuppi, payload, data);
        writer->Fill();
      }
      entries++;
    }
    // close
    double dt = (std::chrono::duration<double>(std::chrono::steady_clock::now() - tstart)).count();
    return makeReport(dt, entries, ins, out);
  }
};

#endif