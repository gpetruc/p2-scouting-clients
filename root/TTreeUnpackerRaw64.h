#ifndef p2_clients_TTreeUnpackerRaw64_h
#define p2_clients_TTreeUnpackerRaw64_h
#include "TTreeUnpackerBase.h"
#include <TBranch.h>

class TTreeUnpackerRaw64 : public TTreeUnpackerBase {
public:
  TTreeUnpackerRaw64() {}
  ~TTreeUnpackerRaw64() override {}

  unsigned long int unpack(const std::vector<std::string> &ins, const std::string &out) const override;

protected:
  struct Data {
    // empty
  };
};

#endif