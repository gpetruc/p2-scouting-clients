#include "UnpackerBase.h"
#include "unpack.h"
#include <chrono>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <cassert>

UnpackerBase::Report UnpackerBase::unpackFiles(const std::vector<std::string> &ins, const std::string &out) {
  assert(!ins.empty());
  bookOutput(out);
  Report ret(0);
  unsigned long int bytes_in = 0;
  uint32_t cmsswRun = 0, cmsswLumi = 0, cmsswNOrbits = 0, cmsswOrbit = 0;

  auto tstart = std::chrono::steady_clock::now();
  std::vector<std::fstream> fins;
  for (auto &in : ins) {
    auto &fin = fins.emplace_back(in, std::ios_base::in | std::ios_base::binary);
    if (!fin.good()) {
      throw std::runtime_error("Error opening " + in + " for input");
    }
    if (cmsswHeaders_) {
      char cmsswFileHeader[32];
      fin.read(cmsswFileHeader, 32);
      if (!fin.good() || (std::string(cmsswFileHeader, cmsswFileHeader + 8) != "RAW_0002") ||
          (*reinterpret_cast<uint16_t *>(&cmsswFileHeader[8]) != 32) ||
          (*reinterpret_cast<uint16_t *>(&cmsswFileHeader[10]) != 20)) {
        throw std::runtime_error("Error reading CMSSW file header from " + in);
      }
      if (cmsswRun == 0) {
        cmsswRun = *reinterpret_cast<uint32_t *>(&cmsswFileHeader[16]);
        cmsswLumi = *reinterpret_cast<uint32_t *>(&cmsswFileHeader[20]);
        cmsswNOrbits = *reinterpret_cast<uint32_t *>(&cmsswFileHeader[12]);
        if (cmsswRun == 0 || cmsswLumi == 0) {
          throw std::runtime_error("Error reading CMSSW file header from " + in + " bad run " +
                                   std::to_string(cmsswRun) + " or lumi " + std::to_string(cmsswLumi));
        }
        printf("CMSSW run %u, lumi %u, with %u orbits.\n", cmsswRun, cmsswLumi, cmsswNOrbits);
      } else if ((cmsswRun != *reinterpret_cast<uint32_t *>(&cmsswFileHeader[16])) ||
                 (cmsswLumi != *reinterpret_cast<uint32_t *>(&cmsswFileHeader[20])) ||
                 (cmsswNOrbits != *reinterpret_cast<uint32_t *>(&cmsswFileHeader[12]))) {
        throw std::runtime_error(
            "Mismatch between CMSSW file headers from " + in + " for " + " run " + std::to_string(cmsswRun) + " vs " +
            std::to_string(*reinterpret_cast<uint32_t *>(&cmsswFileHeader[16])) + " lumi " + std::to_string(cmsswLumi) +
            " vs " + std::to_string(*reinterpret_cast<uint32_t *>(&cmsswFileHeader[20])) + " norbits " +
            std::to_string(cmsswNOrbits) + " vs " +
            std::to_string(*reinterpret_cast<uint32_t *>(&cmsswFileHeader[12])));
      }
      bytes_in += std::filesystem::file_size(in);
    }
    ret.bytes_in = bytes_in;
  }

  uint16_t run;
  uint32_t orbit;
  uint16_t bx;
  bool good;
  uint16_t nwords;
  uint64_t header, payload[1 << 12];

  if (!cmsswHeaders_) {
    // simple loop on events
    for (int ifile = 0, nfiles = fins.size(); fins[ifile].good(); ifile = (ifile == nfiles - 1 ? 0 : ifile + 1)) {
      std::fstream &fin = fins[ifile];
      do {
        fin.read(reinterpret_cast<char *>(&header), sizeof(uint64_t));
      } while (header == 0 && fin.good());
      if (!header)
        continue;
      parseHeader(header, run, bx, orbit, good, nwords);
      if (nwords)
        fin.read(reinterpret_cast<char *>(payload), nwords * sizeof(uint64_t));
      fillEvent(run, orbit, bx, good, nwords, payload);
      ret.entries++;
    }
  } else {
    unsigned int nfiles = fins.size();
    std::vector<unsigned int> sizes(nfiles, 0);
    for (unsigned int iorbit = 0; iorbit < cmsswNOrbits; ++iorbit) {
      // first read all orbit headers
      for (unsigned int ifile = 0; ifile < nfiles && fins[ifile].good(); ++ifile) {
        std::fstream &fin = fins[ifile];
        uint32_t orbitHeader[6];
        fin.read(reinterpret_cast<char *>(orbitHeader), 24);
        if (!fin.good() || (orbitHeader[0] != 6) || (orbitHeader[1] != cmsswRun) || (orbitHeader[2] != cmsswLumi) ||
            (orbitHeader[4] % 8 != 0) || ((ifile != 0) && (orbitHeader[3] != cmsswOrbit))) {
          throw std::runtime_error("Failed reading orbit " + std::to_string(iorbit) + " from file " + ins[ifile]);
        }
        cmsswOrbit = orbitHeader[3];
        sizes[ifile] = orbitHeader[4] >> 3;
      }
      //printf("Will read orbit %u (%u/%u)\n", cmsswOrbit, iorbit, cmsswNOrbits);
      // then loop to read events
      for (unsigned int ifile = 0; fins[ifile].good() && sizes[ifile] > 0;
           ifile = (ifile == nfiles - 1 ? 0 : ifile + 1)) {
        std::fstream &fin = fins[ifile];
        header = 0;
        do {
          fin.read(reinterpret_cast<char *>(&header), sizeof(uint64_t));
          sizes[ifile]--;
        } while (header == 0 && fin.good() && sizes[ifile] > 0);
        if (!header)
          continue;
        parseHeader(header, run, bx, orbit, good, nwords);
        if (orbit != cmsswOrbit)
          throw std::runtime_error("Orbit mismatch between CMSSW " + std::to_string(cmsswOrbit) +
                                   " and 64 bit header " + std::to_string(orbit) + " from file " + ins[ifile]);
        if (nwords)
          fin.read(reinterpret_cast<char *>(payload), nwords * sizeof(uint64_t));
        sizes[ifile] -= nwords;
        fillEvent(run, orbit, bx, good, nwords, payload);
        ret.entries++;
      }
    }  // outer loop on orbits
  }    // version with CMSSW headers
  ret.bytes_out = closeOutput();
  ret.time = (std::chrono::duration<double>(std::chrono::steady_clock::now() - tstart)).count();
  return ret;
}

unsigned long int UnpackerBase::unpackOrbits(
    const std::vector<std::pair<const uint64_t *, const uint64_t *>> &buffersIn) {
  unsigned long int entries = 0;
  uint16_t run;
  uint32_t orbit;
  uint16_t bx;
  bool good;
  uint16_t nwords;

  std::vector<std::pair<const uint64_t *, const uint64_t *>> buffers = buffersIn;
  for (int ibuff = 0, nbuffs = buffers.size(), lbuff = nbuffs - 1; buffers[ibuff].first != buffers[ibuff].second;
       ibuff = (ibuff == lbuff ? 0 : ibuff + 1)) {
    auto &pa = buffers[ibuff];
    while (pa.first != pa.second && *pa.first == 0) {
      pa.first++;
    }
    if (!*pa.first)
      continue;
    parseHeader(*pa.first, run, bx, orbit, good, nwords);
    fillEvent(run, orbit, bx, good, nwords, pa.first + 1);
    pa.first += (nwords + 1);
    entries++;
  }

  return entries;
}