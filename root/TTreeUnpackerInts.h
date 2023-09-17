#ifndef p2_clients_TTreeUnpackerInts_h
#define p2_clients_TTreeUnpackerInts_h
#include "TTreeUnpackerBase.h"

class TTreeUnpackerInts : public TTreeUnpackerBase {
public:
  TTreeUnpackerInts() {}
  ~TTreeUnpackerInts() override {}

  unsigned long int unpack(const std::vector<std::string> &ins, const std::string &out) const override;

protected:
  struct Data {
    //puppi candidate info:
    uint16_t pt[255];
    int16_t eta[255], phi[255];
    uint8_t pid[255];
    uint8_t quality[255];
    //charged only:
    int16_t z0[255];
    int8_t dxy[255];
    //neutral only:
    uint16_t wpuppi[255];
  };
};

#endif