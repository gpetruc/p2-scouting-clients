#ifndef p2_clients_RNTupleUnpackerRaw64_h
#define p2_clients_RNTupleUnpackerRaw64_h
#include "RNTupleUnpackerBase.h"
#include <ROOT/RVec.hxx>

class RNTupleUnpackerRaw64 : public RNTupleUnpackerBase {
public:
  RNTupleUnpackerRaw64() {}
  ~RNTupleUnpackerRaw64() override {}

  void bookOutput(const std::string &out) override final;

  void fillEvent(
      uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) override final;

protected:
  std::shared_ptr<ROOT::RVec<uint64_t>> p_data;
};

#endif