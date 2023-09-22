#ifndef p2_clients_GMTTkMuTTreeUnpackerInts_h
#define p2_clients_GMTTkMuTTreeUnpackerInts_h
#include "TTreeUnpackerBase.h"

class GMTTkMuTTreeUnpackerInts : public TTreeUnpackerBase {
public:
  GMTTkMuTTreeUnpackerInts() {}
  ~GMTTkMuTTreeUnpackerInts() override {}

  Report unpack(const std::vector<std::string> &ins, const std::string &out) const override;

protected:
  struct Data {
    uint16_t nmu;
    uint16_t pt[255];
    int16_t eta[255];
    int16_t phi[255];
    int8_t charge[255];
    int16_t z0[255];
    int16_t d0[255];
    uint8_t quality[255];
    uint8_t isolation[255];
    uint8_t beta[255];
  };
};

#endif