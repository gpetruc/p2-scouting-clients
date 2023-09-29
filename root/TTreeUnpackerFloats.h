#ifndef p2_clients_TTreeUnpackerFloats_h
#define p2_clients_TTreeUnpackerFloats_h
#include "TTreeUnpackerBase.h"

class TTreeUnpackerFloats : public TTreeUnpackerBase {
public:
  TTreeUnpackerFloats(const std::string &floatType) : floatType_(floatType) {}
  ~TTreeUnpackerFloats() override {}

  void bookOutput(const std::string &out) override final;

  void fillEvent(
      uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) override final;

protected:
  std::string floatType_;

  struct Data {
    //puppi candidate info:
    float pt[255];
    float eta[255], phi[255];
    short int pdgid[255];
    uint8_t quality[255];
    //charged only:
    float z0[255];
    float dxy[255];
    //neutral only:
    float wpuppi[255];
  };

  Data data_;
};

#endif