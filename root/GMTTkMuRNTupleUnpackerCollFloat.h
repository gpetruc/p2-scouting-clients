#ifndef p2_clients_GMTTkMuRNTupleUnpackerCollFloat_h
#define p2_clients_GMTTkMuRNTupleUnpackerCollFloat_h
#include "RNTupleUnpackerBase.h"

class GMTTkMuRNTupleUnpackerCollFloat : public RNTupleUnpackerBase {
public:
  GMTTkMuRNTupleUnpackerCollFloat() {}
  ~GMTTkMuRNTupleUnpackerCollFloat() override {}

  void bookOutput(const std::string &out) override final;

  void fillEvent(
      uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) override final;

protected:
  struct Data {
    std::shared_ptr<float> p_pt, p_eta, p_phi, p_z0, p_d0, p_beta;
    std::shared_ptr<int8_t> p_charge;
    std::shared_ptr<uint8_t> p_quality, p_isolation;
    std::shared_ptr<ROOT::Experimental::RCollectionNTupleWriter> subwriter;
  };

  Data data_;
};

#endif