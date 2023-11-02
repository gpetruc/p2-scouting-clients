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
inline bool readpid(const uint64_t data, short int &pdgid) {
  uint8_t pid = (data >> 37) & 0x7;
  assignpdgid(pid, pdgid);
  return (pid > 1);
}
inline bool readpid(const uint64_t data,
                    short int &pdgid_c,
                    short int &pdgid_n) {  //overload for "charged/neutral" version
  uint8_t pid = (data >> 37) & 0x7;
  if (pid > 1) {
    assignpdgid(pid, pdgid_c);
    return true;
  } else {
    assignpdgid(pid, pdgid_n);
    return false;
  }
}
inline void readshared(const uint64_t data, uint16_t &pt, int16_t &eta, int16_t &phi) {  //int
  pt = data & 0x3FFF;
  eta = ((data >> 25) & 1) ? ((data >> 14) | (-0x800)) : ((data >> 14) & (0xFFF));
  phi = ((data >> 36) & 1) ? ((data >> 26) | (-0x400)) : ((data >> 26) & (0x7FF));
}
inline void readshared(const uint64_t data, float &pt, float &eta, float &phi) {  //float
  uint16_t ptint = data & 0x3FFF;
  pt = ptint * 0.25;

  int etaint = ((data >> 25) & 1) ? ((data >> 14) | (-0x800)) : ((data >> 14) & (0xFFF));
  eta = etaint * M_PI / 720.;

  int phiint = ((data >> 36) & 1) ? ((data >> 26) | (-0x400)) : ((data >> 26) & (0x7FF));
  phi = phiint * M_PI / 720.;
}
inline void readcharged(const uint64_t data, int16_t &z0, int8_t &dxy, uint16_t &quality) {  //int
  z0 = ((data >> 49) & 1) ? ((data >> 40) | (-0x200)) : ((data >> 40) & 0x3FF);

  dxy = ((data >> 57) & 1) ? ((data >> 50) | (-0x100)) : ((data >> 50) & 0xFF);
  quality = (data >> 58) & 0x7;  //3 bits
}
inline void readcharged(const uint64_t data, float &z0, float &dxy, uint16_t &quality) {  //float
  int z0int = ((data >> 49) & 1) ? ((data >> 40) | (-0x200)) : ((data >> 40) & 0x3FF);
  z0 = z0int * .05f;  //conver to centimeters

  int dxyint = ((data >> 57) & 1) ? ((data >> 50) | (-0x100)) : ((data >> 50) & 0xFF);
  dxy = dxyint * 0.05f;           // PLACEHOLDER
  quality = (data >> 58) & 0x7;  //3 bits
}
inline void readcharged(const uint64_t data, int16_t &z0, int8_t &dxy, uint8_t &quality) {  //int
  z0 = ((data >> 49) & 1) ? ((data >> 40) | (-0x200)) : ((data >> 40) & 0x3FF);

  dxy = ((data >> 57) & 1) ? ((data >> 50) | (-0x100)) : ((data >> 50) & 0xFF);
  quality = (data >> 58) & 0x7;  //3 bits
}
inline void readcharged(const uint64_t data, float &z0, float &dxy, uint8_t &quality) {  //float
  int z0int = ((data >> 49) & 1) ? ((data >> 40) | (-0x200)) : ((data >> 40) & 0x3FF);
  z0 = z0int * .05f;  //conver to centimeters

  int dxyint = ((data >> 57) & 1) ? ((data >> 50) | (-0x100)) : ((data >> 50) & 0xFF);
  dxy = dxyint * 0.05f;           // PLACEHOLDER
  quality = (data >> 58) & 0x7;  //3 bits
}
inline void readneutral(const uint64_t data, uint16_t &wpuppi, uint16_t &id) {
  wpuppi = (data >> 40) & 0x3FF;
  id = (data >> 50) & 0x3F;
}
inline void readneutral(const uint64_t data, float &wpuppi, uint16_t &id) {
  int wpuppiint = (data >> 40) & 0x3FF;
  wpuppi = wpuppiint * float(1 / 256.f);
  id = (data >> 50) & 0x3F;
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
inline void readevent(std::fstream &fin,
                      uint64_t &header,
                      uint16_t &run,
                      uint16_t &bx,
                      uint32_t &orbit,
                      bool &good,
                      uint16_t &npuppi,
                      uint64_t (&data)[255],
                      uint16_t (&pt)[255],
                      int16_t (&eta)[255],
                      int16_t (&phi)[255],
                      uint16_t (&pid)[255],
                      int16_t (&z0)[255],
                      int8_t (&dxy)[255],
                      uint16_t (&quality)[255],
                      uint16_t (&wpuppi)[255],
                      uint16_t (&id)[255]) {  //int, combined
  readheader(fin, header, run, bx, orbit, good, npuppi);
  if (npuppi)
    fin.read(reinterpret_cast<char *>(&data[0]), npuppi * sizeof(uint64_t));
  for (uint16_t i = 0; i < npuppi; ++i) {
    readshared(data[i], pt[i], eta[i], phi[i]);
    pid[i] = (data[i] >> 37) & 0x7;
    if (pid[i] > 1) {
      readcharged(data[i], z0[i], dxy[i], quality[i]);
      wpuppi[i] = 0;
      id[i] = 0;
    } else {
      readneutral(data[i], wpuppi[i], id[i]);
      z0[i] = 0;
      dxy[i] = 0;
      quality[i] = 0;
    }
  }
}
inline void readevent(std::fstream &fin,
                      uint64_t &header,
                      uint16_t &run,
                      uint16_t &bx,
                      uint32_t &orbit,
                      bool &good,
                      uint16_t &npuppi,
                      uint16_t &npuppi_c,
                      uint16_t &npuppi_n,
                      uint64_t (&data)[255],
                      uint16_t (&pt_c)[255],
                      uint16_t (&pt_n)[255],
                      int16_t (&eta_c)[255],
                      int16_t (&eta_n)[255],
                      int16_t (&phi_c)[255],
                      int16_t (&phi_n)[255],
                      uint16_t (&pid_c)[255],
                      uint16_t (&pid_n)[255],
                      int16_t (&z0)[255],
                      int8_t (&dxy)[255],
                      uint16_t (&quality)[255],
                      uint16_t (&wpuppi)[255],
                      uint16_t (&id)[255]) {  //int, separate
  npuppi_c = 0;
  npuppi_n = 0;
  readheader(fin, header, run, bx, orbit, good, npuppi);
  if (npuppi)
    fin.read(reinterpret_cast<char *>(&data[0]), npuppi * sizeof(uint64_t));
  for (uint16_t i = 0; i < npuppi; ++i) {
    uint16_t pid = (data[i] >> 37) & 0x7;
    if (pid > 1) {
      pid_c[i] = pid;
      readshared(data[i], pt_c[npuppi_c], eta_c[npuppi_c], phi_c[npuppi_c]);
      readcharged(data[i], z0[npuppi_c], dxy[npuppi_c], quality[npuppi_c]);
      npuppi_c++;
    } else {
      pid_n[i] = pid;
      readshared(data[i], pt_n[npuppi_n], eta_n[npuppi_n], phi_n[npuppi_n]);
      readneutral(data[i], wpuppi[npuppi_n], id[npuppi_n]);
      npuppi_n++;
    }
  }
}
inline void readevent(std::fstream &fin,
                      uint64_t &header,
                      uint16_t &run,
                      uint16_t &bx,
                      uint32_t &orbit,
                      bool &good,
                      uint16_t &npuppi,
                      uint64_t (&data)[255],
                      float (&pt)[255],
                      float (&eta)[255],
                      float (&phi)[255],
                      short int (&pdgid)[255],
                      float (&z0)[255],
                      float (&dxy)[255],
                      uint16_t (&quality)[255],
                      float (&wpuppi)[255],
                      uint16_t (&id)[255]) {  //float, combined
  readheader(fin, header, run, bx, orbit, good, npuppi);
  if (npuppi)
    fin.read(reinterpret_cast<char *>(&data[0]), npuppi * sizeof(uint64_t));
  for (uint16_t i = 0; i < npuppi; ++i) {
    readshared(data[i], pt[i], eta[i], phi[i]);
    if (readpid(data[i], pdgid[i])) {
      readcharged(data[i], z0[i], dxy[i], quality[i]);
      wpuppi[i] = 0;
      id[i] = 0;
    } else {
      readneutral(data[i], wpuppi[i], id[i]);
      z0[i] = 0;
      dxy[i] = 0;
      quality[i] = 0;
    }
  }
}
inline void readevent(std::fstream &fin,
                      uint64_t &header,
                      uint16_t &run,
                      uint16_t &bx,
                      uint32_t &orbit,
                      bool &good,
                      uint8_t &npuppi,
                      uint64_t *data,
                      float *pt,
                      float *eta,
                      float *phi,
                      short int *pdgid,
                      float *z0,
                      float *dxy,
                      float *wpuppi,
                      uint8_t *quality) {  //float, combined
  readheader(fin, header, run, bx, orbit, good, npuppi);
  if (npuppi)
    fin.read(reinterpret_cast<char *>(&data[0]), npuppi * sizeof(uint64_t));
  for (unsigned int i = 0, n = npuppi; i < n; ++i) {
    readshared(data[i], pt[i], eta[i], phi[i]);
    if (readpid(data[i], pdgid[i])) {
      readcharged(data[i], z0[i], dxy[i], quality[i]);
      wpuppi[i] = 0;
    } else {
      readneutral(data[i], wpuppi[i], quality[i]);
      z0[i] = 0;
      dxy[i] = 0;
    }
  }
}
inline void readevent(std::fstream &fin,
                      uint64_t &header,
                      uint16_t &run,
                      uint16_t &bx,
                      uint32_t &orbit,
                      bool &good,
                      uint8_t &npuppi,
                      uint64_t *data,
                      std::vector<float> &pt,
                      std::vector<float> &eta,
                      std::vector<float> &phi,
                      std::vector<short int> &pdgid,
                      std::vector<float> &z0,
                      std::vector<float> &dxy,
                      std::vector<float> &wpuppi,
                      std::vector<uint8_t> &quality) {  //float, combined
  readheader(fin, header, run, bx, orbit, good, npuppi);
  if (npuppi)
    fin.read(reinterpret_cast<char *>(&data[0]), npuppi * sizeof(uint64_t));
  unsigned int n = npuppi;
  pt.resize(n);
  eta.resize(n);
  phi.resize(n);
  pdgid.resize(n);
  z0.resize(n);
  dxy.resize(n);
  wpuppi.resize(n);
  quality.resize(n);
  for (unsigned int i = 0, n = npuppi; i < n; ++i) {
    readshared(data[i], pt[i], eta[i], phi[i]);
    if (readpid(data[i], pdgid[i])) {
      readcharged(data[i], z0[i], dxy[i], quality[i]);
      wpuppi[i] = 0;
    } else {
      readneutral(data[i], wpuppi[i], quality[i]);
      z0[i] = 0;
      dxy[i] = 0;
    }
  }
}

inline void readevent(std::fstream &fin,
                      uint64_t &header,
                      uint16_t &run,
                      uint16_t &bx,
                      uint32_t &orbit,
                      bool &good,
                      uint16_t &npuppi,
                      uint16_t &npuppi_c,
                      uint16_t &npuppi_n,
                      uint64_t (&data)[255],
                      float (&pt_c)[255],
                      float (&pt_n)[255],
                      float (&eta_c)[255],
                      float (&eta_n)[255],
                      float (&phi_c)[255],
                      float (&phi_n)[255],
                      short int (&pdgid_c)[255],
                      short int (&pdgid_n)[255],
                      float (&z0)[255],
                      float (&dxy)[255],
                      uint16_t (&quality)[255],
                      float (&wpuppi)[255],
                      uint16_t (&id)[255]) {  //float, separate
  npuppi_c = 0;
  npuppi_n = 0;
  readheader(fin, header, run, bx, orbit, good, npuppi);
  if (npuppi)
    fin.read(reinterpret_cast<char *>(&data[0]), npuppi * sizeof(uint64_t));
  for (uint i = 0; i < npuppi; ++i) {
    if (readpid(data[i], pdgid_c[npuppi_c], pdgid_n[npuppi_n])) {
      readshared(data[i], pt_c[npuppi_c], eta_c[npuppi_c], phi_c[npuppi_c]);
      readcharged(data[i], z0[npuppi_c], dxy[npuppi_c], quality[npuppi_c]);
      npuppi_c++;
    } else {
      readshared(data[i], pt_n[npuppi_n], eta_n[npuppi_n], phi_n[npuppi_n]);
      readneutral(data[i], wpuppi[npuppi_n], id[npuppi_n]);
      npuppi_n++;
    }
  }
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

inline void decode_gmt_tkmu(const uint16_t nwords,
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
                            uint8_t beta[255]) {
  nmu = (nwords * 2) / 3;
  const uint32_t *ptr32 = reinterpret_cast<const uint32_t *>(data);
  for (uint16_t i = 0; i < nmu; ++i, ptr32 += 3) {
    uint64_t wlo;
    uint32_t whi;
    if ((i & 1) == 0) {
      wlo = *reinterpret_cast<const uint64_t *>(ptr32);
      whi = *(ptr32 + 2);
    } else {
      wlo = *reinterpret_cast<const uint64_t *>(ptr32 + 1);
      whi = *ptr32;
    }
    pt[i] = extractBitsFromW<1, 16>(wlo);
    phi[i] = extractSignedBitsFromW<17, 13>(wlo);
    eta[i] = extractSignedBitsFromW<30, 14>(wlo);
    z0[i] = extractSignedBitsFromW<44, 10>(wlo);
    d0[i] = extractSignedBitsFromW<54, 10>(wlo);
    charge[i] = (whi & 1) ? -1 : +1;
    quality[i] = extractBitsFromW<1, 4>(whi);
    isolation[i] = extractBitsFromW<9, 4>(whi);
    beta[i] = extractBitsFromW<13, 4>(whi);
  }
}
inline void decode_gmt_tkmu(const uint16_t nwords,
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
                            float beta[255]) {
  nmu = (nwords * 2) / 3;
  const uint32_t *ptr32 = reinterpret_cast<const uint32_t *>(data);
  for (uint16_t i = 0; i < nmu; ++i, ptr32 += 3) {
    uint64_t wlo;
    uint32_t whi;
    if ((i & 1) == 0) {
      wlo = *reinterpret_cast<const uint64_t *>(ptr32);
      whi = *(ptr32 + 2);
    } else {
      wlo = *reinterpret_cast<const uint64_t *>(ptr32 + 1);
      whi = *ptr32;
    }
    pt[i] = extractBitsFromW<1, 16>(wlo) * 0.03125f;
    phi[i] = extractSignedBitsFromW<17, 13>(wlo) * float(M_PI / (1 << 12));
    eta[i] = extractSignedBitsFromW<30, 14>(wlo) * float(M_PI / (1 << 12));
    z0[i] = extractSignedBitsFromW<44, 10>(wlo) * 0.05f;
    d0[i] = extractSignedBitsFromW<54, 10>(wlo) * 0.03f;
    charge[i] = (whi & 1) ? -1 : +1;
    quality[i] = extractBitsFromW<1, 8>(whi);
    isolation[i] = extractBitsFromW<9, 4>(whi);
    beta[i] = extractBitsFromW<13, 4>(whi) * 0.06f;
  }
}

inline void printReport(const UnpackerBase::Report &rep) {
  float inrate = rep.bytes_in / (1024. * 1024.) / rep.time;
  printf(
      "Done in %.2fs. Event rate: %.1f kHz (40 MHz / %.1f), input data rate %.1f MB/s (%.1f "
      "Gbps)\n",
      rep.time,
      rep.entries / rep.time / 1000.,
      (40e6 * rep.time / rep.entries),
      inrate,
      inrate * 8 / 1024.);
  if (rep.bytes_out) {
    float outrate = rep.bytes_out / (1024. * 1024.) / rep.time;
    float ratio = rep.bytes_out / rep.bytes_in;
    printf(
        "Input file size: %.2f MB, Output file size: %.2f MB, File size ratio: %.3f, output data rate %.1f MB/s "
        "(%.1f "
        "Gbps)\n\n",
        rep.bytes_in / (1024. * 1024.),
        rep.bytes_out / (1024. * 1024.),
        ratio,
        outrate,
        outrate * 8 / 1024);
  } else {
    printf("\n");
  }
}

inline UnpackerBase::Report makeReport(float treal,
                                       unsigned long int entries,
                                       const std::vector<std::string> &infiles,
                                       const std::string &outfile) {
  float insize = 0, outsize = 0;
  struct stat stat_buf;
  for (auto &infile : infiles) {
    int rc = stat(infile.c_str(), &stat_buf);
    insize += ((rc == 0) ? stat_buf.st_size : 0);
  };
  if (!outfile.empty()) {
    int rc = stat(outfile.c_str(), &stat_buf);
    outsize = ((rc == 0) ? stat_buf.st_size : 0);
  }
  return UnpackerBase::Report(entries, treal, insize, outsize);
}
#endif
