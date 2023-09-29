#ifndef p2_clients_TTreeUnpackerRaw64_h
#define p2_clients_TTreeUnpackerRaw64_h
#include "TTreeUnpackerBase.h"
#include <TBranch.h>

class TTreeUnpackerRaw64 : public TTreeUnpackerBase {
public:
  TTreeUnpackerRaw64() : branch_(nullptr) {}
  ~TTreeUnpackerRaw64() override {}

  void bookOutput(const std::string &out) override final;

  void fillEvent(
      uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) override final;

protected:
  TBranch *branch_;
};

#endif