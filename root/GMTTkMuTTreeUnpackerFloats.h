#ifndef p2_clients_GMTTkMuTTreeUnpackerFloats_h
#define p2_clients_GMTTkMuTTreeUnpackerFloats_h
#include "TTreeUnpackerBase.h"

class GMTTkMuTTreeUnpackerFloats : public TTreeUnpackerBase {
public:
  GMTTkMuTTreeUnpackerFloats(const std::string &floatType) : floatType_(floatType) {}
  ~GMTTkMuTTreeUnpackerFloats() override {}

  Report unpack(const std::vector<std::string> &ins, const std::string &out) const override;

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
};

#endif