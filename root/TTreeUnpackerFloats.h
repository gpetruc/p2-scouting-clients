#ifndef p2_clients_TTreeUnpackerFloats_h
#define p2_clients_TTreeUnpackerFloats_h
#include "TTreeUnpackerBase.h"

class TTreeUnpackerFloats : public TTreeUnpackerBase {
public:
  TTreeUnpackerFloats(const std::string &floatType) : floatType_(floatType) {}
  ~TTreeUnpackerFloats() override {}

  unsigned long int unpack(const std::vector<std::string> &ins, const std::string &out) const override;

protected:
  std::string floatType_;

  struct Data {
    //puppi candidate info:
    float pt[255];
    float eta[255], phi[255];
    short int pdgid[255];
    //charged only:
    float z0[255];
    float dxy[255];
    //neutral only:
    float wpuppi[255];
    //common only:
    uint8_t quality[255];
  };
};

#endif