#ifndef clients_root_puppi_h
#define clients_root_puppi_h
#include <cstdint>

struct Puppi {
  float pt, eta, phi, z0, dxy, wpuppi;
  short int pdgid;
  uint8_t quality;
};
struct PuppiInt {
  uint16_t pt;
  int16_t eta, phi, z0;
  int8_t dxy;
  short int pdgid;
  uint8_t wpuppi, quality;
};

#endif