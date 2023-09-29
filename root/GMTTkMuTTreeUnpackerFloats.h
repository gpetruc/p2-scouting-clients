#ifndef p2_clients_GMTTkMuTTreeUnpackerFloats_h
#define p2_clients_GMTTkMuTTreeUnpackerFloats_h
#include "TTreeUnpackerBase.h"

class GMTTkMuTTreeUnpackerFloats : public TTreeUnpackerBase {
public:
  GMTTkMuTTreeUnpackerFloats(const std::string &floatType) : floatType_(floatType) {}
  ~GMTTkMuTTreeUnpackerFloats() override {}

  void bookOutput(const std::string &out) override final;

  void fillEvent(
      uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) override final;

protected:
  std::string floatType_;

  struct Data {
    uint16_t nmu;
    float pt[255];
    float eta[255];
    float phi[255];
    int8_t charge[255];
    float z0[255];
    float d0[255];
    uint8_t quality[255];
    uint8_t isolation[255];
    float beta[255];
  };

  Data data_;
};

#endif