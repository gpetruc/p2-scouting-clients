#include <cstdio>
#include <cstdint>
#include <fstream>
#include <math.h>
#include <sys/stat.h>

template <typename U>
inline void parseHeader(const uint64_t &header, uint16_t &run, uint16_t &bx, uint32_t &orbit, bool &good, U &npuppi) {
  npuppi = header & 0xFFF;
  bx = (header >> 12) & 0xFFF;
  orbit = (header >> 24) & 0X3FFFFFFF;
  run = (header >> 54);
  good = (header & (1llu << 61));
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
  //if (pid == 0) pdgid = 130;
  //else if (pid == 1) pdgid = 22;
  //else if (pid == 2) pdgid = -211;
  //else if (pid == 3) pdgid = 211;
  //else if (pid == 4) pdgid = 11;
  //else if (pid == 5) pdgid = -11;
  //else if (pid == 6) pdgid = 13;
  //else if (pid == 7) pdgid = -13;
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
  z0 = z0int * .05;  //conver to centimeters

  int dxyint = ((data >> 57) & 1) ? ((data >> 50) | (-0x100)) : ((data >> 50) & 0xFF);
  dxy = dxyint * 0.05;           // PLACEHOLDER
  quality = (data >> 58) & 0x7;  //3 bits
}
inline void readcharged(const uint64_t data, int16_t &z0, int8_t &dxy, uint8_t &quality) {  //int
  z0 = ((data >> 49) & 1) ? ((data >> 40) | (-0x200)) : ((data >> 40) & 0x3FF);

  dxy = ((data >> 57) & 1) ? ((data >> 50) | (-0x100)) : ((data >> 50) & 0xFF);
  quality = (data >> 58) & 0x7;  //3 bits
}
inline void readcharged(const uint64_t data, float &z0, float &dxy, uint8_t &quality) {  //float
  int z0int = ((data >> 49) & 1) ? ((data >> 40) | (-0x200)) : ((data >> 40) & 0x3FF);
  z0 = z0int * .05;  //conver to centimeters

  int dxyint = ((data >> 57) & 1) ? ((data >> 50) | (-0x100)) : ((data >> 50) & 0xFF);
  dxy = dxyint * 0.05;           // PLACEHOLDER
  quality = (data >> 58) & 0x7;  //3 bits
}
inline void readneutral(const uint64_t data, uint16_t &wpuppi, uint16_t &id) {
  wpuppi = (data >> 23) & 0x3FF;
  id = (data >> 13) & 0x3F;
}
inline void readneutral(const uint64_t data, float &wpuppi, uint16_t &id) {
  int wpuppiint = (data >> 23) & 0x3FF;
  wpuppi = wpuppiint / 256;
  id = (data >> 13) & 0x3F;
}
inline void readneutral(const uint64_t data, uint16_t &wpuppi, uint8_t &id) {
  wpuppi = (data >> 23) & 0x3FF;
  id = (data >> 13) & 0x3F;
}
inline void readneutral(const uint64_t data, float &wpuppi, uint8_t &id) {
  int wpuppiint = (data >> 23) & 0x3FF;
  wpuppi = wpuppiint / 256;
  id = (data >> 13) & 0x3F;
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

inline void report(double tcpu, double treal, int entries, float insize, float outsize) {
  float inrate = insize / (1024. * 1024.) / treal;
  printf(
      "Done in %.2fs (cpu), %.2fs (real). Event rate: %.1f kHz (40 MHz / %.1f), input data rate %.1f MB/s (%.1f "
      "Gbps)\n",
      tcpu,
      treal,
      entries / treal / 1000.,
      (40e6 * treal / entries),
      inrate,
      inrate * 8 / 1024.);
  if (outsize) {
    float outrate = outsize / (1024. * 1024.) / treal;
    float ratio = outsize / insize;
    printf(
        "Input file size: %.2f MB, Output file size: %.2f MB, File size ratio: %.3f, output data rate %.1f MB/s (%.1f "
        "Gbps)\n\n",
        insize / (1024. * 1024.),
        outsize / (1024. * 1024.),
        ratio,
        outrate,
        outrate * 8 / 1024);
  } else {
    printf("\n");
  }
}

inline void report(double tcpu, double treal, int entries, const char *infile, const char *outfile) {
  struct stat stat_buf;
  int rc = stat(infile, &stat_buf);
  float insize = ((rc == 0) ? stat_buf.st_size : 0), outsize = 0;
  if (outfile) {
    rc = stat(outfile, &stat_buf);
    outsize = ((rc == 0) ? stat_buf.st_size : 0);
  }
  report(tcpu, treal, entries, insize, outsize);
}
