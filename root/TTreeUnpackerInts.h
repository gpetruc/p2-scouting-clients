#ifndef p2_clients_TTreeUnpackerInts_h
#define p2_clients_TTreeUnpackerInts_h
#include "TTreeUnpackerBase.h"

class TTreeUnpackerInts : public TTreeUnpackerBase {
public:
  TTreeUnpackerInts() {}
  ~TTreeUnpackerInts() override {}

  void bookOutput(const std::string &out) override final;

  void fillEvent(
      uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) override final;

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

  Data data_;
};

#endif