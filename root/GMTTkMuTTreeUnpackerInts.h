#ifndef p2_clients_GMTTkMuTTreeUnpackerInts_h
#define p2_clients_GMTTkMuTTreeUnpackerInts_h
#include "TTreeUnpackerBase.h"

class GMTTkMuTTreeUnpackerInts : public TTreeUnpackerBase {
public:
  GMTTkMuTTreeUnpackerInts() {}
  ~GMTTkMuTTreeUnpackerInts() override {}

  void bookOutput(const std::string &out) override final;

  void fillEvent(
      uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) override final;

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

  Data data_;
};

#endif