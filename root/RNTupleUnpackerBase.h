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

  void setThreads(unsigned int threads) override;
  void setCompression(const std::string &algo, unsigned int level) override;
  unsigned long int closeOutput() override;

protected:
  int compression_;
  std::string fout_;
  std::unique_ptr<ROOT::Experimental::RNTupleWriter> writer_;
  struct DataBase {
    std::shared_ptr<uint16_t> p_run, p_bx;
    std::shared_ptr<uint32_t> p_orbit;
    std::shared_ptr<bool> p_good;
  };
  DataBase dataBase_;

  std::unique_ptr<ROOT::Experimental::RNTupleModel> modelBase() {
    auto model = ROOT::Experimental::RNTupleModel::Create();
    dataBase_.p_run = model->MakeField<uint16_t>("run");
    dataBase_.p_orbit = model->MakeField<uint32_t>("orbit");
    dataBase_.p_bx = model->MakeField<uint16_t>("bx");
    dataBase_.p_good = model->MakeField<bool>("good");
    return model;
  }

  void bookBase(const std::string &fout, std::unique_ptr<ROOT::Experimental::RNTupleModel> model);
};

#endif