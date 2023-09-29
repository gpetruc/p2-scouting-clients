#ifndef p2_clients_RNTupleUnpackerCollInt_h
#define p2_clients_RNTupleUnpackerCollInt_h
#include "RNTupleUnpackerBase.h"

class RNTupleUnpackerCollInt : public RNTupleUnpackerBase {
public:
  RNTupleUnpackerCollInt() {}
  ~RNTupleUnpackerCollInt() override {}

  void bookOutput(const std::string &out) override final;

  void fillEvent(
      uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) override final;

protected:
  struct Data {
    std::shared_ptr<uint16_t> p_pt;
    std::shared_ptr<int16_t> p_eta, p_phi;
    std::shared_ptr<uint8_t> p_pid, p_quality;
    std::shared_ptr<int16_t> p_z0;
    std::shared_ptr<int8_t> p_dxy;
    std::shared_ptr<uint16_t> p_wpuppi;
    std::shared_ptr<ROOT::Experimental::RCollectionNTupleWriter> subwriter;
  };

  Data data_;
};

#endif