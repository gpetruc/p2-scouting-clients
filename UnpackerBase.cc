#include "UnpackerBase.h"
#include "unpack.h"
#include <chrono>
#include <filesystem>
#include <fstream>

UnpackerBase::Report UnpackerBase::unpackFiles(const std::vector<std::string> &ins, const std::string &out) {
  bookOutput(out);
  Report ret(0);
  unsigned long int bytes_in = 0;

  auto tstart = std::chrono::steady_clock::now();
  std::vector<std::fstream> fins;
  for (auto &in : ins) {
    fins.emplace_back(in, std::ios_base::in | std::ios_base::binary);
    if (!fins.back().good()) {
      throw std::runtime_error("Error opening " + in + " for otput");
    }
    bytes_in += std::filesystem::file_size(in);
  }
  ret.bytes_in = bytes_in;

  uint16_t run;
  uint32_t orbit;
  uint16_t bx;
  bool good;
  uint16_t nwords;
  uint64_t header, payload[1 << 12];

  // loop
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