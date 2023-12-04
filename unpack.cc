#include "unpack.h"

void unpack_puppi_ints(uint16_t nwords,
                       const uint64_t *words,
                       uint16_t *__restrict__ pt,
                       int16_t *__restrict__ eta,
                       int16_t *__restrict__ phi,
                       uint8_t *__restrict__ pid,
                       uint8_t *__restrict__ quality,
                       int16_t *__restrict__ z0,
                       int8_t *__restrict__ dxy,
                       uint16_t *__restrict__ wpuppi) {
  for (uint16_t i = 0; i < nwords; ++i) {
    readshared(words[i], pt[i], eta[i], phi[i]);
    uint8_t id = (words[i] >> 37) & 0x7;
    readcharged(words[i], id, z0[i], dxy[i]);
    readneutral(words[i], id, wpuppi[i]);
    readquality(words[i], id, quality[i]);
    pid[i] = id;
  }
}

void unpack_puppi_floats(uint16_t nwords,
                         const uint64_t *words,
                         float *__restrict__ pt,
                         float *__restrict__ eta,
                         float *__restrict__ phi,
                         short int *__restrict__ pdgid,
                         uint8_t *__restrict__ quality,
                         float *__restrict__ z0,
                         float *__restrict__ dxy,
                         float *__restrict__ wpuppi) {
  for (uint16_t i = 0; i < nwords; ++i) {
    readshared(words[i], pt[i], eta[i], phi[i]);
    uint8_t pid = (words[i] >> 37) & 0x7;
    readcharged(words[i], pid, z0[i], dxy[i]);
    readneutral(words[i], pid, wpuppi[i]);
    readquality(words[i], pid, quality[i]);
    vassignpdgid(pid, pdgid[i]);
  }
}

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
                     uint8_t beta[255]) {
  nmu = (nwords * 2) / 3;
  const uint32_t *ptr32 = reinterpret_cast<const uint32_t *>(data);
  for (uint16_t i = 0, n = nmu; i < n; ++i) {
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
    ptr32 += 3;
  }
}

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

void printReport(const UnpackerBase::Report &rep) {
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

UnpackerBase::Report makeReport(float treal,
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