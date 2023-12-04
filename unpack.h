#ifndef p2_clients_unpack_h
#define p2_clients_unpack_h
#include <cstdio>
#include <cstdint>
#include <fstream>
#include <math.h>
#include <sys/stat.h>
#include "UnpackerBase.h"

template <typename U>
inline void parseHeader(const uint64_t &header, uint16_t &run, uint16_t &bx, uint32_t &orbit, bool &good, U &npuppi) {
  npuppi = header & 0xFFF;
  bx = (header >> 12) & 0xFFF;
  orbit = (header >> 24) & 0X3FFFFFFF;
  run = (header >> 54);
  good = !(header & (1llu << 61));
}
inline void readheader(
    std::fstream &fin, uint64_t &header, uint16_t &run, uint16_t &bx, uint32_t &orbit, bool &good, uint16_t &npuppi) {
  fin.read(reinterpret_cast<char *>(&header), sizeof(uint64_t));
  parseHeader(header, run, bx, orbit, good, npuppi);
}
inline void readheader(
    std::fstream &fin, uint64_t &header, uint16_t &run, uint16_t &bx, uint32_t &orbit, bool &good, uint8_t &npuppi) {
  fin.read(reinterpret_cast<char *>(&header), sizeof(uint64_t));
  parseHeader(header, run, bx, orbit, good, npuppi);
}
inline void assignpdgid(uint8_t pid, short int &pdgid) {
  static constexpr int16_t PDGIDS[8] = {130, 22, -211, 211, 11, -11, 13, -13};
  pdgid = PDGIDS[pid];
}
inline void vassignpdgid(uint8_t pid, short int &pdgid) {
  // vectorizable version, ugly as it is...
  short int pdgId = pid ? 22 : 130;
  if (pid > 1) {  // charged
    if (pid / 2 == 1)
      pdgId = -211;
    else if (pid / 2 == 2)
      pdgId = 11;
    else
      pdgId = 13;
    if (pid & 1)
      pdgId = -pdgId;
  }
  pdgid = pdgId;
}
inline bool readpid(const uint64_t data, short int &pdgid) {
  uint8_t pid = (data >> 37) & 0x7;
  assignpdgid(pid, pdgid);
  return (pid > 1);
}

inline void readshared(const uint64_t data, uint16_t &pt, int16_t &eta, int16_t &phi) {  //int
  pt = data & 0x3FFF;
  eta = ((data >> 25) & 1) ? ((data >> 14) | (-0x800)) : ((data >> 14) & (0xFFF));
  phi = ((data >> 36) & 1) ? ((data >> 26) | (-0x400)) : ((data >> 26) & (0x7FF));
}
inline void readshared(const uint64_t data, float &pt, float &eta, float &phi) {  //float
  uint16_t ptint = data & 0x3FFF;
  pt = ptint * 0.25f;

  int etaint = ((data >> 25) & 1) ? ((data >> 14) | (-0x800)) : ((data >> 14) & (0xFFF));
  eta = etaint * float(M_PI / 720.);

  int phiint = ((data >> 36) & 1) ? ((data >> 26) | (-0x400)) : ((data >> 26) & (0x7FF));
  phi = phiint * float(M_PI / 720.);
}

inline void readcharged(const uint64_t data, int16_t &z0, int8_t &dxy, uint8_t &quality) {  //int
  z0 = ((data >> 49) & 1) ? ((data >> 40) | (-0x200)) : ((data >> 40) & 0x3FF);

  dxy = ((data >> 57) & 1) ? ((data >> 50) | (-0x100)) : ((data >> 50) & 0xFF);
  quality = (data >> 58) & 0x7;  //3 bits
}

inline void readcharged(const uint64_t data, uint8_t pid, int16_t &z0, int8_t &dxy) {  //int
  int16_t z0c = ((data >> 49) & 1) ? ((data >> 40) | (-0x200)) : ((data >> 40) & 0x3FF);
  int8_t dxyc = ((data >> 57) & 1) ? ((data >> 50) | (-0x100)) : ((data >> 50) & 0xFF);
  z0 = pid > 1 ? z0c : int16_t(0);
  dxy = pid > 1 ? dxyc : int8_t(0);
}

inline void readcharged(const uint64_t data, float &z0, float &dxy, uint8_t &quality) {  //float
  int z0int = ((data >> 49) & 1) ? ((data >> 40) | (-0x200)) : ((data >> 40) & 0x3FF);
  z0 = z0int * .05f;  //conver to centimeters

  int dxyint = ((data >> 57) & 1) ? ((data >> 50) | (-0x100)) : ((data >> 50) & 0xFF);
  dxy = dxyint * 0.05f;          // PLACEHOLDER
  quality = (data >> 58) & 0x7;  //3 bits
}

inline void readcharged(const uint64_t data, uint8_t pid, float &z0, float &dxy) {  //float
  int z0int = ((data >> 49) & 1) ? ((data >> 40) | (-0x200)) : ((data >> 40) & 0x3FF);
  z0 = (pid > 1) * z0int * .05f;  //conver to centimeters

  int dxyint = ((data >> 57) & 1) ? ((data >> 50) | (-0x100)) : ((data >> 50) & 0xFF);
  dxy = (pid > 1) * dxyint * 0.05f;  // PLACEHOLDER
}

inline void readneutral(const uint64_t data, uint16_t &wpuppi, uint8_t &id) {
  wpuppi = (data >> 40) & 0x3FF;
  id = (data >> 50) & 0x3F;
}
inline void readneutral(const uint64_t data, float &wpuppi, uint8_t &id) {
  int wpuppiint = (data >> 40) & 0x3FF;
  wpuppi = wpuppiint * float(1 / 256.f);
  id = (data >> 50) & 0x3F;
}
inline void readneutral(const uint64_t data, uint8_t pid, uint16_t &wpuppi) {
  uint16_t wpuppiint = (data >> 40) & 0x3FF;
  wpuppi = pid > 1 ? wpuppiint : uint16_t(256);
}
inline void readneutral(const uint64_t data, uint8_t pid, float &wpuppi) {
  int wpuppiint = (data >> 40) & 0x3FF;
  wpuppi = pid > 1 ? wpuppiint * float(1 / 256.f) : 1.0f;
}
inline void readquality(const uint64_t data, uint8_t pid, uint8_t &quality) {
  quality = pid > 1 ? (data >> 58) & 0x7 : (data >> 50) & 0x3F;
}

template <unsigned int start, unsigned int bits = 16, typename T>
inline uint16_t extractBitsFromW(const T word) {
  return (word >> start) & ((1 << bits) - 1);
}
template <unsigned int start, unsigned int bits = 16, typename T>
inline int16_t extractSignedBitsFromW(const T word) {
  uint16_t raw = extractBitsFromW<start, bits>(word);
  if ((bits < 16) && (raw & (1 << (bits - 1)))) {
    constexpr uint16_t ormask = (1 << 16) - (1 << bits);
    raw |= ormask;
  }
  return raw;
}

void unpack_puppi_ints(uint16_t nwords,
                       const uint64_t *words,
                       uint16_t *__restrict__ pt,
                       int16_t *__restrict__ eta,
                       int16_t *__restrict__ phi,
                       uint8_t *__restrict__ pid,
                       uint8_t *__restrict__ quality,
                       int16_t *__restrict__ z0,
                       int8_t *__restrict__ dxy,
                       uint16_t *__restrict__ wpuppi);

void unpack_puppi_floats(uint16_t nwords,
                         const uint64_t *words,
                         //puppi candidate info:
                         float *__restrict__ pt,
                         float *__restrict__ eta,
                         float *__restrict__ phi,
                         short int *__restrict__ pdgid,
                         uint8_t *__restrict__ quality,
                         //charged only:
                         float *__restrict__ z0,
                         float *__restrict__ dxy,
                         //neutral only:
                         float *__restrict__ wpuppi);

void decode_gmt_tkmu(const uint16_t nwords,
                     const uint64_t data[255],
                     uint16_t &nmu,
                     uint16_t pt[255],
                     int16_t eta[255],
                     int16_t phi[255],
                     int8_t charge[255],
                     int16_t z0[255],
                     int16_t d0[255],
                     uint8_t quality[255],
                     uint8_t isolation[255],
                     uint8_t beta[255]);

void decode_gmt_tkmu(const uint16_t nwords,
                     const uint64_t data[255],
                     uint16_t &nmu,
                     float pt[255],
                     float eta[255],
                     float phi[255],
                     int8_t charge[255],
                     float z0[255],
                     float d0[255],
                     uint8_t quality[255],
                     uint8_t isolation[255],
                     float beta[255]);

void printReport(const UnpackerBase::Report &rep);

inline UnpackerBase::Report makeReport(float treal,
                                       unsigned long int entries,
                                       const std::vector<std::string> &infiles,
                                       const std::string &outfile);
#endif
