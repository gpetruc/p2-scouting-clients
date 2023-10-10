#include <cstdio>
#include <cstdint>
#include <fstream>
#include <string>
#include <chrono>
#include <cstdlib>
#include <cassert>
#include <cmath>
#include <exception>
#include <vector>

#include <fcntl.h>
#include <unistd.h>
#include <string.h>

#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <thread>
#include <atomic>
#include <vector>

#include <getopt.h>

typedef std::chrono::time_point<std::chrono::steady_clock> tp;

template <unsigned int I>
inline bool test_bit(const uint8_t *t) {
  return t[I / 8] & (1 << (I % 8));
}

class CheckerBase {
public:
  CheckerBase() {}

  virtual int run(int src) = 0;
  virtual ~CheckerBase(){};

  void init(unsigned int tmux, unsigned int firstBx = 0, unsigned int orbitMux = 1, unsigned int maxorbits = 10000000) {
    maxorbits_ = maxorbits;
    tmux_ = tmux;
    firstbx_ = firstBx;
    orbitMux_ = orbitMux;
    ndebug_ = 0;
    id_ = firstbx_;
    clear();
  }
  void clear() {
    events_ = 0;
    orbits_ = 0;
    truncevents_ = 0;
    truncevents_orbit_ = 0;
    truncorbits_ = 0;
    puppis_ = 0;
    oldbx_ = std::numeric_limits<unsigned int>::max();
    oldorbit_ = std::numeric_limits<unsigned int>::max();
    oldrecord_ = std::numeric_limits<uint64_t>::max();
  }
  void setDebug(int ndebug = 5) { ndebug_ = ndebug; }
  void setId(unsigned int id) { id_ = id; }

  static void print128(const uint8_t *start) {
    for (unsigned int i = 0; i < 16; ++i)
      printf("%02x ", unsigned(start[i]));
  }
  static void print256(const uint8_t *start) {
    for (unsigned int i = 0; i < 32; ++i)
      printf("%02x ", unsigned(start[i]));
  }

  struct DTH_Header {
    uint8_t size128;
    bool ok, start, end;
  };
  struct DTH_Header256 {
    uint8_t size256;
    bool ok, start, end;
  };

  DTH_Header readDTH(int sockfd, uint8_t out[16], bool checkStart = false, bool checkEnd = false) {
    int n = read(sockfd, reinterpret_cast<char *>(out), 16);
    assert(n <= 0 || n == 16);
    bool ok = (n == 16);
    return parseDTH(ok, out, checkStart, checkEnd);
  }
  DTH_Header parseDTH(bool ok, const uint8_t out[16], bool checkStart = false, bool checkEnd = false) {
    DTH_Header ret;
    ret.ok = ok;
    if (!ret.ok)
      return ret;
    ret.start = test_bit<47>(out);
    ret.end = test_bit<46>(out);
    ret.size128 = out[48 / 8];
    if (events_ < ndebug_) {
      printf("DTH header: start %d, end %d, length128 %u: ", int(ret.start), int(ret.end), unsigned(ret.size128));
      print128(out);
      printf("\n");
    }
    assert(out[0] == 0x47 && out[1] == 0x5a);
    assert(ret.end || (ret.size128 == 0xFF));
    if (checkStart)
      assert(ret.start);
    if (checkEnd)
      assert(ret.end);
    return ret;
  }
  DTH_Header256 readDTH256(int sockfd, uint8_t out[32], bool checkStart = false, bool checkEnd = false) {
    int t = 32, i = 0;
    bool ok = true;
    do {
      int n = read(sockfd, reinterpret_cast<char *>(&out[i]), t);
      if (n <= 0) {
        ok = false;
        break;
      }
      t -= n;
      i += n;
    } while (t > 0);
    return parseDTH256(ok, out, checkStart, checkEnd);
  }
  DTH_Header256 parseDTH256(bool ok, const uint8_t out[32], bool checkStart = false, bool checkEnd = false) {
    DTH_Header256 ret;
    ret.ok = ok;
    if (!ret.ok)
      return ret;
    ret.start = test_bit<47>(out);
    ret.end = test_bit<46>(out);
    ret.size256 = out[48 / 8];
    if (events_ < ndebug_) {
      printf("DTH header: start %d, end %d, length256 %u: ", int(ret.start), int(ret.end), unsigned(ret.size256));
      print256(out);
      printf("\n");
    }
    if (!(out[0] == 0x47 && out[1] == 0x5a)) {
      printf("Bad DTH256 header, missing magic: start %d, end %d, length256 %u:",
             int(ret.start),
             int(ret.end),
             unsigned(ret.size256));
      print256(out);
      printf("\n");
      throw std::runtime_error(std::to_string(id_) + ": Bad DTH256 header, missing magic");
      ret.start = false;
      ret.end = false;
      ret.size256 = 0xff;
    }
    if (!(ret.end || (ret.size256 == 0xFF)))
      throw std::runtime_error(std::to_string(id_) + ": Bad DTH256 header, size != 255 but end flag 0");
    if (checkStart)
      if (!ret.start)
        throw std::runtime_error(std::to_string(id_) + ": Bad DTH256 header, expecting start bit set");
    if (checkEnd)
      if (!ret.end)
        throw std::runtime_error(std::to_string(id_) + ": Bad DTH256 header, expecting end bit set");
    return ret;
  }
  uint64_t readSRHeader(int sockfd, uint8_t out[16], bool checkEv = false) {
    int n = read(sockfd, reinterpret_cast<char *>(out), 16);
    assert(n <= 0 || n == 16);
    return n <= 0 ? 0 : parseSRHeader(out, checkEv);
  }
  uint64_t readSRHeader(uint8_t *&ptr, const uint8_t *end, bool checkEv = false) {
    uint64_t ret = (ptr + 16 > end ? 0 : parseSRHeader(ptr, checkEv));
    ptr += 16;
    return ret;
  }
  uint64_t parseSRHeader(const uint8_t out[16], bool checkEv = false) {
    assert(out[15] == 0x55);
    uint64_t evno = *reinterpret_cast<const uint64_t *>(&out[8]) & ((1lu << 48) - 1);
    if (events_ < ndebug_) {
      printf("SR header: evno %lu: ", evno);
      print128(out);
      printf("\n");
    }
    if (checkEv && oldrecord_ != std::numeric_limits<uint64_t>::max() && evno != oldrecord_ + 1) {
      throw std::runtime_error(std::to_string(id_) + ": Record number mismatch, found " + std::to_string(evno) +
                               " after " + std::to_string(oldrecord_));
    }
    oldrecord_ = evno;
    return evno;
  }

  struct SR_Trailer {
    uint32_t orbit;
    uint16_t bx, len;
  };

  SR_Trailer readSRTrailer(int sockfd, uint8_t out[16]) {
    int n = read(sockfd, reinterpret_cast<char *>(out), 16);
    assert(n == 16);
    return parseSRTrailer(out);
  }
  SR_Trailer readSRTrailer(uint8_t *&ptr, const uint8_t *end) {
    assert(ptr + 16 <= end);
    const uint8_t *out = ptr;
    ptr += 16;
    return parseSRTrailer(out);
  }
  SR_Trailer parseSRTrailer(const uint8_t out[16]) {
    SR_Trailer ret;
    ret.orbit = *reinterpret_cast<const uint32_t *>(&out[4]);
    ret.bx = (*reinterpret_cast<const uint16_t *>(&out[8])) & 0xFFF;
    ret.len = ((*reinterpret_cast<const uint32_t *>(&out[9])) >> 4) & 0xFFFFF;
    if (events_ < ndebug_) {
      printf("SR trailer: orbit %u (%x), bx %u, elen %u: ", ret.orbit, ret.orbit, ret.bx, ret.len);
      print128(out);
      printf("\n");
    }
    assert(out[15] == 0xAA);
    return ret;
  }

  struct PuppiHeader {
    uint32_t orbit;
    uint16_t bx;
    uint16_t npuppi;
    bool err;
  };
  struct PuppiOrbitHeader {
    uint32_t orbit;
    uint32_t length;
    bool err;
  };

  PuppiHeader readPuppiHeader(int sockfd, uint64_t &out) {
    int n = read(sockfd, reinterpret_cast<char *>(&out), 8);
    if (n == 0) {
      out = 0;
      return PuppiHeader();
    }
    if (n != 8)
      throw std::runtime_error(std::to_string(id_) + ": Failed reading Event Header, got only " + std::to_string(n) +
                               "/8 bytes.");
    return parsePuppiHeader(out);
  }
  PuppiHeader readPuppiHeaderSkippingZeros(int sockfd, uint64_t &out) {
    do {
      int n = read(sockfd, reinterpret_cast<char *>(&out), 8);
      if (n == 0)
        break;
      if (n != 8)
        throw std::runtime_error(std::to_string(id_) + ": Failed reading Event Header, got only " + std::to_string(n) +
                                 "/8 bytes.");
      if (out != 0)
        return parsePuppiHeader(out);
    } while (good(sockfd));
    out = 0;
    return PuppiHeader();
  }
  PuppiHeader readPuppiHeader(uint8_t *&ptr, const uint8_t *end, uint64_t &out) {
    assert(ptr + 8 <= end);
    out = *reinterpret_cast<const uint64_t *>(ptr);
    ptr += 8;
    return parsePuppiHeader(out);
  }
  PuppiHeader parsePuppiHeader(const uint64_t &out) {
    PuppiHeader ret;
    ret.orbit = (out >> 24) & 0xFFFFFFFF;
    ret.bx = (out >> 12) & 0xFFF;
    ret.npuppi = out & 0xFFF;
    ret.err = (out & (1llu << 61));
    if ((out >> 62) != 0b10)
      throw std::runtime_error(std::to_string(id_) + ": Bad event header found: " + std::to_string(out));
    if (events_ < ndebug_) {
      printf("Event header %016lx, orbit %u (%x), bx %u, npuppi %d, ok %d\n",
             out,
             ret.orbit,
             ret.orbit,
             ret.bx,
             ret.npuppi,
             int(!ret.err));
    }
    return ret;
  }
  PuppiOrbitHeader readPuppiOrbitHeader(int sockfd, uint64_t &out) {
    int n = read(sockfd, reinterpret_cast<char *>(&out), 8);
    if (n == 0) {
      out = 0;
      return PuppiOrbitHeader();
    }
    if (n != 8)
      throw std::runtime_error(std::to_string(id_) + ": Failed reading Event Header, got only " + std::to_string(n) +
                               "/8 bytes.");
    return parsePuppiOrbitHeader(out);
  }
  PuppiOrbitHeader readPuppiOrbitHeader(uint8_t *&ptr, const uint8_t *end, uint64_t &out) {
    assert(ptr + 8 <= end);
    out = *reinterpret_cast<const uint64_t *>(ptr);
    ptr += 8;
    return parsePuppiOrbitHeader(out);
  }
  PuppiOrbitHeader parsePuppiOrbitHeader(const uint64_t &out) {
    PuppiOrbitHeader ret;
    ret.orbit = (out >> 24) & 0xFFFFFFFF;
    ret.length = out & 0xFFFFFF;
    ret.err = (out & (1llu << 61));
    if ((out >> 62) != 0b11)
      throw std::runtime_error(std::to_string(id_) + ": Bad orbit header found: " + std::to_string(out));
    if (events_ < ndebug_) {
      printf("Orbit header %016lx, orbit %u (%x), length %u, ok %d\n",
             out,
             ret.orbit,
             ret.orbit,
             ret.length,
             int(!ret.err));
    }
    return ret;
  }
  uint64_t fillPuppiOrbitHeader(const PuppiOrbitHeader &in) {
    uint64_t out = 0;
    out |= in.length & 0xFFFFFF;
    out |= (uint64_t(in.orbit & 0xFFFFFFFF) << 24);
    out |= uint64_t(in.err) << 61;
    out |= uint64_t(0b11) << 62;
    return out;
  }
  void countEventsAndOrbits(unsigned long orbit, unsigned int bx, bool truncated = false) {
    if (orbit != oldorbit_) {
      if (oldorbit_ != std::numeric_limits<unsigned int>::max() && (orbit != oldorbit_ + orbitMux_)) {
        throw std::runtime_error(std::to_string(id_) + ": Orbit mismatch: found orbit " + std::to_string(orbit) +
                                 " bx " + std::to_string(bx) + " after orbit " + std::to_string(oldorbit_) + " bx " +
                                 std::to_string(oldbx_));
      }
      if (truncevents_orbit_ > 0)
        truncorbits_++;
      truncevents_orbit_ = 0;
      orbits_++;
      oldorbit_ = orbit;
      if (oldbx_ != std::numeric_limits<unsigned int>::max() && bx != firstbx_) {
        throw std::runtime_error(std::to_string(id_) + ": BX mismatch: found " + std::to_string(unsigned(bx)) +
                                 " at beginning of orbit, instead of " + std::to_string(firstbx_));
      }
      oldbx_ = bx;
    } else {
      if (oldbx_ != std::numeric_limits<unsigned int>::max() && bx != oldbx_ + tmux_) {
        throw std::runtime_error(std::to_string(id_) + ": BX mismatch: found " + std::to_string(unsigned(bx)) +
                                 " after " + std::to_string(oldbx_) + ", expected " + std::to_string(oldbx_ + tmux_));
      }
      oldbx_ = bx;
    }
    events_++;
    if (truncated) {
      truncevents_++;
      truncevents_orbit_++;
    }
  }

  void printDone(std::chrono::time_point<std::chrono::steady_clock> tstart,
                 std::chrono::time_point<std::chrono::steady_clock> tend) {
    uint64_t totbytes = (events_ + puppis_) * 8;
    std::chrono::duration<double> dt = tend - tstart;
    double evrate = events_ / dt.count(), orbrate = orbits_ / dt.count(), orbrate_lhc = 40e6 / 3564;
    double dt_lhc = orbits_ / orbrate_lhc;
    double datarate = totbytes / 1024.0 / 1024.0 / 1024.0 / dt.count();
    double fillrate = totbytes / 1024.0 / 1024.0 / 1024.0 / dt_lhc;
    printf("%02u: Read %u orbits, %lu events, %lu candidates, %lu bytes\n",
           id_,
           orbits_.load(),
           events_.load(),
           puppis_.load(),
           totbytes);
    printf("%02u: Time to read %.2f ms, LHC time %.2f ms (x %.3f)\n",
           id_,
           dt.count() * 1000,
           dt_lhc * 1000,
           dt_lhc / dt.count());
    printf("%02u: Orbit rate %.3f kHz (TM1 %.3f kHz), event rate %.2f kHz\n",
           id_,
           orbrate / 1000,
           orbrate_lhc / 1000.,
           evrate / 1000.);
    printf(
        "%02u: Data rate %.3f GB/s, input data rate %.3f GB/s (x %.3f)\n", id_, datarate, fillrate, fillrate / datarate);
    if (truncevents_ || truncorbits_) {
      printf("%02u: Truncated events %lu (%.4f%%), orbits %u (%.4f%%)\n",
             id_,
             truncevents_.load(),
             (truncevents_ * 100.0 / events_),
             truncorbits_.load(),
             (truncorbits_ * 100.0 / orbits_));
    }
    printf("\n");
  }

  bool good(int sockfd) { return sockfd > 0; }

  void skip(int sockfd, int offs) {
    static char *buff[1024];
    assert(offs > 0);
    do {
      int n = read(sockfd, buff, std::min(offs, 1024));
      assert(n > 0);
      offs -= n;
    } while (offs > 0);
  }

protected:
  unsigned int firstbx_, tmux_, orbitMux_, id_;
  std::atomic<uint64_t> events_, truncevents_;
  std::atomic<uint32_t> truncevents_orbit_;
  std::atomic<uint32_t> orbits_, truncorbits_, maxorbits_;
  std::atomic<uint64_t> puppis_;
  unsigned int oldorbit_, oldbx_;
  uint64_t oldrecord_;
  uint64_t ndebug_;
};

class NativeChecker : public CheckerBase {
public:
  NativeChecker(bool padTo128 = true, bool skipZeros = false)
      : CheckerBase(), padTo128_(padTo128), skipZeros_(skipZeros) {}
  ~NativeChecker() override{};

  int run(int in) override {
    int ret = 0;
    uint64_t buff64;
    unsigned int nprint = 100;
    auto tstart = std::chrono::steady_clock::now();
    try {
      while (good(in) && orbits_ <= maxorbits_) {
        PuppiHeader evh = skipZeros_ ? readPuppiHeaderSkippingZeros(in, buff64) : readPuppiHeader(in, buff64);
        if (buff64 == 0)
          break;
        unsigned int n64 = evh.npuppi + 1;
        bool truncated = false;
        if (padTo128_) {
          n64 += n64 % 2;
          if (evh.npuppi == 0) {
            int n = read(in, reinterpret_cast<char *>(&buff64), 8);
            assert(n == 8);
            truncated = (buff64 != 0);
          }
        } else {
          truncated = (evh.npuppi == 0) && evh.err;
        }
        puppis_ += evh.npuppi;
        if (!truncated && n64 > 1)
          skip(in, 8 * (n64 - 1));
        countEventsAndOrbits(evh.orbit, evh.bx, truncated);
        if (events_ % nprint == 0) {
          printf("Read %10lu events, %7u orbits. Truncated %8lu events, %7u orbits\n",
                 events_.load(),
                 orbits_.load(),
                 truncevents_.load(),
                 truncorbits_.load());
          nprint = std::min(nprint << 1, 100000u);
        }
      }
    } catch (const std::exception &e) {
      printf("Terminating an exception was raised:\n%s\n", e.what());
      ret = 1;
    }
    auto tend = std::chrono::steady_clock::now();
    printDone(tstart, tend);
    return ret;
  }

protected:
  bool padTo128_, skipZeros_;
};

class DTHBasicChecker : public CheckerBase {
public:
  DTHBasicChecker() : CheckerBase() {}
  ~DTHBasicChecker() override{};

  int run(int in) override {
    int ret = 0;
    uint8_t buff128[16];
    uint64_t buff64;
    unsigned int nprint = 100;
    auto tstart = std::chrono::steady_clock::now();
    try {
      while (good(in) && orbits_ <= maxorbits_) {
        DTH_Header dthh = readDTH(in, buff128, true, true);
        if (!dthh.ok)
          break;
        readSRHeader(in, buff128, true);
        PuppiHeader evh = readPuppiHeader(in, buff64);
        assert(buff64 != 0);
        unsigned int n64 = evh.npuppi + 1;
        n64 += n64 % 2;
        puppis_ += evh.npuppi;
        skip(in, 8 * (n64 - 1));
        SR_Trailer srt = readSRTrailer(in, buff128);
        assert(dthh.size128 == srt.len);
        assert(evh.orbit == srt.orbit && evh.bx == srt.bx);
        countEventsAndOrbits(evh.orbit, evh.bx);
        if (events_ % nprint == 0) {
          printf("Read %10lu events, %7u orbits\n", events_.load(), orbits_.load());
          nprint *= 2;
        }
      }
    } catch (const std::exception &e) {
      printf("Terminating an exception was raised:\n%s\n", e.what());
      ret = 1;
    }
    auto tend = std::chrono::steady_clock::now();
    printDone(tstart, tend);
    return ret;
  }
};

class DTHBasicCheckerOA : public CheckerBase {
public:
  DTHBasicCheckerOA(unsigned int orbSize_kb, bool checkData = true, bool srHeaders = true)
      : CheckerBase(), orbSize_(orbSize_kb * 1024), checkData_(checkData), srHeaders_(srHeaders) {}
  ~DTHBasicCheckerOA() override{};

  bool readChunk(std::fstream &in, uint8_t *&ptr, unsigned size128) {
    in.read(reinterpret_cast<char *>(ptr), size128 << 4);
    ptr += (size128 << 4);
    return (in.gcount() != 0);
  }

  bool readChunk(int sockfd, uint8_t *&ptr, unsigned size128) {
    size_t toread = size128 << 4;
    while (toread > 0) {
      int n = read(sockfd, reinterpret_cast<char *>(ptr), toread);
      if (n <= 0)
        return false;
      toread -= n;
      ptr += n;
    }
    return true;
  }

  int run(int in) override {
    int ret = 0;
    uint8_t buff128[16];
    uint64_t buff64;
    uint8_t *orbit_buff = reinterpret_cast<uint8_t *>(std::aligned_alloc(4096u, orbSize_));
    unsigned int nprint = 100;
    auto tstart = std::chrono::steady_clock::now();
    bool isfirst = true;
    int toprint = nprint;
    try {
      while (good(in) && orbits_ <= maxorbits_) {
        uint8_t *ptr = orbit_buff, *end = orbit_buff + orbSize_;
        DTH_Header dthh = readDTH(in, buff128, true, false);
        if (isfirst) {
          tstart = std::chrono::steady_clock::now();
          isfirst = false;
        }
        uint32_t totlen128 = dthh.size128;
        assert(ptr + (dthh.size128 << 4) < end);
        dthh.ok = readChunk(in, ptr, dthh.size128);
        while (dthh.ok && !dthh.end) {
          dthh = readDTH(in, buff128, false, false);
          if (!dthh.ok)
            break;
          totlen128 += dthh.size128;
          assert(ptr + (dthh.size128 << 4) < end);
          dthh.ok = readChunk(in, ptr, dthh.size128);
        }
        if (!dthh.ok)
          break;
        if (!checkData_) {
          orbits_++;
          if (totlen128 <= 2)
            truncorbits_++;
          puppis_ += totlen128 * 2;
          if (--toprint == 0) {
            printf("Read %7u orbits\n", orbits_.load());
            nprint = std::min(nprint << 1, 10000u);
            toprint = nprint;
          }
          continue;
        }
        ptr = orbit_buff;
        end = ptr + (totlen128 << 4);
        uint64_t orbitno = 0, trunclen128, tail8;
        if (srHeaders_) {
          orbitno = readSRHeader(ptr, end, false);
          if (orbitno == 0)
            break;
          trunclen128 = 2;
          tail8 = 16;
        } else {
          trunclen128 = 1;
          tail8 = 0;
        }
        if (totlen128 == trunclen128) {  // truncated orbit
          if (srHeaders_) {
            SR_Trailer srt = readSRTrailer(ptr, end);
            assert(totlen128 == srt.len);
            orbitno = srt.orbit;
          } else {
            PuppiHeader evh = readPuppiHeader(ptr, end, buff64);
            orbitno = evh.orbit;
            ptr++;
          }
          assert(oldorbit_ == std::numeric_limits<unsigned int>::max() || orbitno == oldorbit_ + 1);
          oldorbit_ = orbitno;
          orbits_ += 1;
          truncorbits_ += 1;
          continue;
        }

        while (ptr + tail8 < end) {
          PuppiHeader evh = readPuppiHeader(ptr, end, buff64);
          unsigned int n64 = evh.npuppi + 1;
          n64 += n64 & 1;
          puppis_ += evh.npuppi;
          bool truncated = (evh.npuppi == 0) && (*ptr != 0);
          ptr += (n64 - 1) << 3;
          countEventsAndOrbits(evh.orbit, evh.bx, truncated);
        }
        if (srHeaders_) {
          SR_Trailer srt = readSRTrailer(ptr, end);
          assert(totlen128 == srt.len);
          assert(oldorbit_ == std::numeric_limits<unsigned int>::max() || srt.orbit == oldorbit_);
        }
        if (--toprint == 0) {
          if (truncevents_) {
            printf("%02u: Read %10lu events, %7u orbits. Truncated %8lu events, %7u orbits\n",
                   id_,
                   events_.load(),
                   orbits_.load(),
                   truncevents_.load(),
                   truncorbits_.load());
          } else {
            printf("%02u:Read %10lu events, %7u orbits\n", id_, events_.load(), orbits_.load());
          }
          nprint = std::min(nprint << 1, 10000u);
          toprint = nprint;
        }
      }
    } catch (const std::exception &e) {
      printf("%02u:Terminating an exception was raised:\n%s\n", id_, e.what());
      ret = 1;
    }
    auto tend = std::chrono::steady_clock::now();
    printDone(tstart, tend);
    std::free(orbit_buff);
    return ret;
  }

protected:
  unsigned int orbSize_;
  bool checkData_, srHeaders_;
};

class DTHReceiveOA : public DTHBasicCheckerOA {
public:
  DTHReceiveOA(unsigned int orbSize_kb, const char *fname, unsigned int prescale, unsigned long maxSize_gb = 4)
      : DTHBasicCheckerOA(orbSize_kb),
        prescale_(prescale),
        maxSize_(maxSize_gb << 30),
        fname_(fname),
        fout_(fname, std::ios_base::binary | std::ios_base::out | std::ios_base::trunc) {}
  ~DTHReceiveOA() override {}

  int run(int in) override {
    int ret = 0;
    uint8_t buff128[16];
    uint8_t *orbit_buff = reinterpret_cast<uint8_t *>(std::aligned_alloc(4096u, orbSize_));
    unsigned int nprint = 100;
    auto tstart = std::chrono::steady_clock::now();
    bool isfirst = true;
    int toprint = nprint;
    unsigned long int readBytes = 0, wroteBytes = 0;
    try {
      while (good(in) && orbits_ <= maxorbits_ && wroteBytes < maxSize_) {
        uint8_t *ptr = orbit_buff, *end = orbit_buff + orbSize_;
        DTH_Header dthh = readDTH(in, buff128, true, false);
        if (isfirst) {
          tstart = std::chrono::steady_clock::now();
          isfirst = false;
        }
        uint32_t totlen128 = dthh.size128;
        assert(ptr + (dthh.size128 << 4) < end);
        dthh.ok = readChunk(in, ptr, dthh.size128);
        while (dthh.ok && !dthh.end) {
          dthh = readDTH(in, buff128, false, false);
          if (!dthh.ok)
            break;
          totlen128 += dthh.size128;
          assert(ptr + (dthh.size128 << 4) < end);
          dthh.ok = readChunk(in, ptr, dthh.size128);
        }
        if (!dthh.ok)
          break;
        orbits_++;
        if (totlen128 <= 2)
          truncorbits_++;
        readBytes += totlen128 << 4;
        if (orbits_ % prescale_ == 0) {
          fout_.write(reinterpret_cast<char *>(orbit_buff + 16), (totlen128 - 2) << 4);
          wroteBytes += totlen128 << 4;
        }
        if (--toprint == 0) {
          printf("%02u: Read %7u orbits (%7u truncated, %.4f%%), %9.3f GB. Wrote %6.3f GB.\n",
                 id_,
                 orbits_.load(),
                 truncorbits_.load(),
                 truncorbits_.load() * 100.0 / orbits_.load(),
                 readBytes / (1024. * 1024. * 1024.),
                 wroteBytes / (1024. * 1024. * 1024.));
          nprint = std::min(nprint << 1, 10000u);
          toprint = nprint;
        }
      }
    } catch (const std::exception &e) {
      printf("%02u: Terminating an exception was raised:\n%s\n", id_, e.what());
      ret = 1;
    }
    auto tend = std::chrono::steady_clock::now();
    std::chrono::duration<double> dt = tend - tstart;
    double readRate = readBytes / 1024.0 / 1024.0 / 1024.0 / dt.count();
    double wroteRate = wroteBytes / 1024.0 / 1024.0 / 1024.0 / dt.count();
    printf("%02u: Read %u orbits (%u truncated, %.4f%%), %.3f GB\n",
           id_,
           orbits_.load(),
           truncorbits_.load(),
           truncorbits_.load() * 100.0 / orbits_.load(),
           readBytes / (1024. * 1024. * 1024.));
    printf("%02u: Wrote %.3f GB to %s\n", id_, wroteBytes / (1024. * 1024. * 1024.), fname_.c_str());
    printf("%02u: Data rate in %.3f GB/s, out %.3f GB/s\n", id_, readRate, wroteRate);
    printf("\n");
    std::free(orbit_buff);
    return ret;
  }

protected:
  unsigned int prescale_;
  unsigned long int maxSize_;
  std::string fname_;
  std::fstream fout_;
  bool checkData_;
};

class DTHBasicChecker256 : public CheckerBase {
public:
  DTHBasicChecker256(unsigned int orbSize_kb, bool checkData = true, bool trailZeros = false)
      : CheckerBase(), orbSize_(orbSize_kb * 1024), checkData_(checkData), trailZeros_(trailZeros) {}
  ~DTHBasicChecker256() override{};

  bool readChunk(int sockfd, uint8_t *&ptr, unsigned size256, bool plusOne) {
    size_t toread = (plusOne ? size256 + 1 : size256) << 5;
    while (toread > 0) {
      int n = read(sockfd, reinterpret_cast<char *>(ptr), toread);
      if (n <= 0)
        return false;
      toread -= n;
      ptr += n;
    }
    if (plusOne)
      ptr -= 32;
    return true;
  }

  int run(int in) override {
    int ret = 0;
    uint8_t buff256[32];
    uint64_t buff64;
    uint8_t *orbit_buff = reinterpret_cast<uint8_t *>(std::aligned_alloc(4096u, orbSize_));
    unsigned int nprint = 100;
    auto tstart = std::chrono::steady_clock::now();
    bool isfirst = true;
    int toprint = nprint;
    try {
      while (good(in) && orbits_ <= maxorbits_) {
        uint8_t *ptr = orbit_buff, *end = orbit_buff + orbSize_;
        DTH_Header256 dthh = readDTH256(in, buff256, true, false);
        if (isfirst) {
          tstart = std::chrono::steady_clock::now();
          isfirst = false;
        }
        uint32_t totlen256 = dthh.size256;
        assert(ptr + (dthh.size256 << 5) < end);
        try {
          dthh.ok = readChunk(in, ptr, dthh.size256, !dthh.end);
          while (dthh.ok && !dthh.end) {
            dthh = parseDTH256(dthh.ok, ptr, false, false);
            if (!dthh.ok)
              break;
            totlen256 += dthh.size256;
            assert(ptr + (dthh.size256 << 5) < end);
            dthh.ok = readChunk(in, ptr, dthh.size256, !dthh.end);
          }
        } catch (const std::exception &e) {
          printf("%02u: Exception was raised in reading DTH frame:\n%s\n", id_, e.what());
          std::fstream dth_debug_dump("dth_debug.dump",
                                      std::ios_base::binary | std::ios_base::out | std::ios_base::trunc);
          dth_debug_dump.write(reinterpret_cast<char *>(orbit_buff), (totlen256 << 5));
          buff64 = 0x4441454444414544;
          dth_debug_dump.write(reinterpret_cast<char *>(&buff64), 8);
          dth_debug_dump.write(reinterpret_cast<char *>(&buff64), 8);
          dth_debug_dump.write(reinterpret_cast<char *>(buff256), 32);
          for (int i = 0; i < 5; ++i) {
            int n = read(in, reinterpret_cast<char *>(orbit_buff), orbSize_);
            if (n <= 0)
              break;
            dth_debug_dump.write(reinterpret_cast<char *>(orbit_buff), n);
          }
          printf("Dumped some data in dth_debug.dump\n");
          ret = 1;
          throw e;
        }
        if (!dthh.ok)
          break;
        if (!checkData_) {
          orbits_++;
          if (totlen256 == 1)
            truncorbits_++;
          puppis_ += totlen256 * 4;
          if (--toprint == 0) {
            printf("Read %7u orbits\n", orbits_.load());
            nprint = std::min(nprint << 1, 10000u);
            toprint = nprint;
          }
          continue;
        }
        ptr = orbit_buff;
        end = ptr + (totlen256 << 5);
        if (totlen256 <= 1) {  // truncated orbit
          PuppiOrbitHeader oh = readPuppiOrbitHeader(ptr, end, buff64);
          uint32_t orbitno = oh.orbit;
          if (!(oldorbit_ == std::numeric_limits<unsigned int>::max() || orbitno == oldorbit_ + orbitMux_)) {
            printf("%02u: Orbit number mismatch, found %u after %u (orbit header %016lx, err %d)\n",
                   id_,
                   orbitno,
                   oldorbit_,
                   buff64,
                   oh.err);
            orbitno = oldorbit_ + orbitMux_;
          }
          assert(oldorbit_ == std::numeric_limits<unsigned int>::max() || orbitno == oldorbit_ + orbitMux_);
          oldorbit_ = orbitno;
          orbits_ += 1;
          truncorbits_ += 1;
          continue;
        }
        while (ptr < end) {
          PuppiHeader evh = readPuppiHeader(ptr, end, buff64);
          unsigned int n64 = evh.npuppi + 1;
          puppis_ += evh.npuppi;
          bool truncated = (evh.npuppi == 0) && evh.err;  // check error bit
          ptr += (n64 - 1) << 3;
          countEventsAndOrbits(evh.orbit, evh.bx, truncated);
          while (trailZeros_ && ptr + 7 < end && ((*reinterpret_cast<const uint64_t *>(ptr)) == 0)) {
            ptr += 8;
          }
          if (ptr + 7 < end && ptr + 3 * 8 >= end) {  // 1-3 words remaining, they may be nulls
            if ((*reinterpret_cast<const uint64_t *>(ptr)) == 0) {
              break;
            }
          }
        }
        if (--toprint == 0) {
          if (truncevents_ || truncorbits_) {
            printf("%02u: Read %10lu events, %7u orbits. Truncated %8lu events, %7u orbits\n",
                   id_,
                   events_.load(),
                   orbits_.load(),
                   truncevents_.load(),
                   truncorbits_.load());
          } else {
            printf("%02u: Read %10lu events, %7u orbits\n", id_, events_.load(), orbits_.load());
          }
          nprint = std::min(nprint << 1, 10000u);
          toprint = nprint;
        }
      }
    } catch (const std::exception &e) {
      printf("%02u:Terminating an exception was raised:\n%s\n", id_, e.what());
      ret = 1;
    }
    auto tend = std::chrono::steady_clock::now();
    printDone(tstart, tend);
    std::free(orbit_buff);
    return ret;
  }

protected:
  unsigned int orbSize_;
  bool checkData_, trailZeros_;
};

class DTHReceive256 : public DTHBasicChecker256 {
public:
  DTHReceive256(unsigned int orbSize_kb, const char *fname, unsigned int prescale, unsigned long maxSize_gb = 4)
      : DTHBasicChecker256(orbSize_kb),
        prescale_(prescale),
        maxSize_(maxSize_gb << 30),
        fname_(fname),
        fout_(fname, std::ios_base::binary | std::ios_base::out | std::ios_base::trunc) {}

  ~DTHReceive256() override {}

  int run(int in) override {
    int ret = 0;
    uint8_t buff256[32];
    uint64_t buff64;
    uint8_t *orbit_buff = reinterpret_cast<uint8_t *>(std::aligned_alloc(4096u, orbSize_));
    unsigned int nprint = 100;
    auto tstart = std::chrono::steady_clock::now();
    bool isfirst = true;
    int toprint = nprint;
    unsigned long int readBytes = 0, wroteBytes = 0;
    try {
      while (good(in) && orbits_ <= maxorbits_ && wroteBytes < maxSize_) {
        uint8_t *ptr = orbit_buff, *end = orbit_buff + orbSize_;
        DTH_Header256 dthh = readDTH256(in, buff256, true, false);
        if (isfirst) {
          tstart = std::chrono::steady_clock::now();
          isfirst = false;
        }
        uint32_t totlen256 = dthh.size256;
        assert(ptr + (dthh.size256 << 5) < end);
        try {
          dthh.ok = readChunk(in, ptr, dthh.size256, !dthh.end);
          while (dthh.ok && !dthh.end) {
            dthh = parseDTH256(dthh.ok, ptr, false, false);
            if (!dthh.ok)
              break;
            totlen256 += dthh.size256;
            assert(ptr + (dthh.size256 << 5) < end);
            dthh.ok = readChunk(in, ptr, dthh.size256, !dthh.end);
          }
        } catch (const std::exception &e) {
          printf("%02u: Exception was raised in reading DTH frame:\n%s\n", id_, e.what());
          std::fstream dth_debug_dump("dth_debug.dump",
                                      std::ios_base::binary | std::ios_base::out | std::ios_base::trunc);
          dth_debug_dump.write(reinterpret_cast<char *>(orbit_buff), (totlen256 << 5));
          buff64 = 0x4441454444414544;
          dth_debug_dump.write(reinterpret_cast<char *>(&buff64), 8);
          dth_debug_dump.write(reinterpret_cast<char *>(&buff64), 8);
          dth_debug_dump.write(reinterpret_cast<char *>(buff256), 32);
          for (int i = 0; i < 5; ++i) {
            int n = read(in, reinterpret_cast<char *>(orbit_buff), orbSize_);
            if (n <= 0)
              break;
            dth_debug_dump.write(reinterpret_cast<char *>(orbit_buff), n);
          }
          printf("Dumped some data in dth_debug.dump\n");
          ret = 1;
          throw e;
        }
        if (!dthh.ok)
          break;
        orbits_++;
        ptr = orbit_buff;
        end = ptr + (totlen256 << 5);
        PuppiOrbitHeader oh;
        if (totlen256 <= 1) {
          truncorbits_++;
          if (totlen256 == 1) {
            oh = readPuppiOrbitHeader(ptr, end, buff64);
          } else {
            oh.length = 0;
            oh.orbit = 0;
            oh.err = true;
          }
        } else {
          PuppiHeader evh = readPuppiHeader(ptr, end, buff64);
          oh.length = totlen256 << 5;
          oh.orbit = evh.orbit;
          oh.err = false;
        }
        readBytes += totlen256 << 5;
        if (orbits_ % prescale_ == 0) {
          fout_.write(reinterpret_cast<char *>(orbit_buff), totlen256 << 5);
          wroteBytes += totlen256 << 5;
        }
        if (--toprint == 0) {
          printf("%02u: Read %7u orbits (%7u truncated, %.4f%%), %9.3f GB. Wrote %6.3f GB.\n",
                 id_,
                 orbits_.load(),
                 truncorbits_.load(),
                 truncorbits_.load() * 100.0 / orbits_.load(),
                 readBytes / (1024. * 1024. * 1024.),
                 wroteBytes / (1024. * 1024. * 1024.));
          nprint = std::min(nprint << 1, 10000u);
          toprint = nprint;
        }
      }
    } catch (const std::exception &e) {
      printf("%02u: Terminating an exception was raised:\n%s\n", id_, e.what());
      ret = 1;
    }
    auto tend = std::chrono::steady_clock::now();
    std::chrono::duration<double> dt = tend - tstart;
    double readRate = readBytes / 1024.0 / 1024.0 / 1024.0 / dt.count();
    double wroteRate = wroteBytes / 1024.0 / 1024.0 / 1024.0 / dt.count();
    printf("%02u: Read %u orbits (%u truncated, %.4f%%), %.3f GB\n",
           id_,
           orbits_.load(),
           truncorbits_.load(),
           truncorbits_.load() * 100.0 / orbits_.load(),
           readBytes / (1024. * 1024. * 1024.));
    printf("%02u: Wrote %.3f GB to %s\n", id_, wroteBytes / (1024. * 1024. * 1024.), fname_.c_str());
    printf("%02u: Data rate in %.3f GB/s, out %.3f GB/s\n", id_, readRate, wroteRate);
    printf("\n");
    std::free(orbit_buff);
    return ret;
  }

protected:
  unsigned int prescale_;
  unsigned long int maxSize_;
  std::string fname_;
  std::fstream fout_;
};

class DTHRollingReceive256 : public DTHBasicChecker256 {
public:
  DTHRollingReceive256(unsigned int orbSize_kb,
                       const char *basepath,
                       unsigned int orbitsPerFile,
                       unsigned int prescale = 1)
      : DTHBasicChecker256(orbSize_kb),
        orbitsPerFile_(orbitsPerFile),
        prescale_(prescale),
        basepath_(basepath),
        fname_(),
        fout_() {}

  ~DTHRollingReceive256() override {}

  void newFile(uint32_t orbitNo) {
    if (fout_.is_open()) {
      assert(fname_.length() > 4);
      rename(fname_.c_str(), fname_.substr(0, fname_.length() - 4).c_str());
      printf("Moved %s -> %s\n", fname_.c_str(), fname_.substr(0, fname_.length() - 4).c_str());
    }
    fout_.close();
    char buff[1024];
    snprintf(buff, 1023, "%s.ts%02d.orb%08u.dump.tmp", basepath_.c_str(), firstbx_, orbitNo);
    fname_ = buff;
    fout_.open(buff, std::ios_base::binary | std::ios_base::out | std::ios_base::trunc);
    printf("%02u: opening output file %s\n", id_, buff);
  }

  int run(int in) override {
    int ret = 0;
    uint8_t buff256[32];
    uint64_t buff64;
    uint8_t *orbit_buff = reinterpret_cast<uint8_t *>(std::aligned_alloc(4096u, orbSize_));
    unsigned int nprint = 100;
    auto tstart = std::chrono::steady_clock::now();
    bool isfirst = true;
    int toprint = nprint;
    unsigned long int readBytes = 0, wroteBytes = 0;
    try {
      while (good(in) && orbits_ <= maxorbits_) {
        uint8_t *ptr = orbit_buff, *end = orbit_buff + orbSize_;
        DTH_Header256 dthh = readDTH256(in, buff256, true, false);
        if (isfirst) {
          tstart = std::chrono::steady_clock::now();
          isfirst = false;
        }
        uint32_t totlen256 = dthh.size256;
        assert(ptr + (dthh.size256 << 5) < end);
        try {
          dthh.ok = readChunk(in, ptr, dthh.size256, !dthh.end);
          while (dthh.ok && !dthh.end) {
            dthh = parseDTH256(dthh.ok, ptr, false, false);
            if (!dthh.ok)
              break;
            totlen256 += dthh.size256;
            assert(ptr + (dthh.size256 << 5) < end);
            dthh.ok = readChunk(in, ptr, dthh.size256, !dthh.end);
          }
        } catch (const std::exception &e) {
          printf("%02u: Exception was raised in reading DTH frame:\n%s\n", id_, e.what());
          std::fstream dth_debug_dump("dth_debug.dump",
                                      std::ios_base::binary | std::ios_base::out | std::ios_base::trunc);
          dth_debug_dump.write(reinterpret_cast<char *>(orbit_buff), (totlen256 << 5));
          buff64 = 0x4441454444414544;
          dth_debug_dump.write(reinterpret_cast<char *>(&buff64), 8);
          dth_debug_dump.write(reinterpret_cast<char *>(&buff64), 8);
          dth_debug_dump.write(reinterpret_cast<char *>(buff256), 32);
          for (int i = 0; i < 5; ++i) {
            int n = read(in, reinterpret_cast<char *>(orbit_buff), orbSize_);
            if (n <= 0)
              break;
            dth_debug_dump.write(reinterpret_cast<char *>(orbit_buff), n);
          }
          printf("Dumped some data in dth_debug.dump\n");
          ret = 1;
          throw e;
        }
        if (!dthh.ok)
          break;
        orbits_++;
        ptr = orbit_buff;
        end = ptr + (totlen256 << 5);
        PuppiOrbitHeader oh;
        if (totlen256 <= 1) {
          truncorbits_++;
          if (totlen256 == 1) {
            oh = readPuppiOrbitHeader(ptr, end, buff64);
          } else {
            oh.length = 0;
            oh.orbit = 0;
            oh.err = true;
          }
        } else {
          PuppiHeader evh = readPuppiHeader(ptr, end, buff64);
          oh.length = totlen256 << 5;
          oh.orbit = evh.orbit;
          oh.err = false;
        }
        readBytes += totlen256 << 5;
        if (!fout_.is_open() || oh.orbit % orbitsPerFile_ == orbitMux_)
          newFile(oh.orbit);
        if (prescale_ != 0 && (prescale_ == 1 || oh.orbit % prescale_ == orbitMux_)) {
          fout_.write(reinterpret_cast<char *>(orbit_buff), totlen256 << 5);
          wroteBytes += totlen256 << 5;
        }
        if (--toprint == 0) {
          printf("%02u: Read %7u orbits (%7u truncated, %.4f%%), %9.3f GB. Wrote %6.3f GB.\n",
                 id_,
                 orbits_.load(),
                 truncorbits_.load(),
                 truncorbits_.load() * 100.0 / orbits_.load(),
                 readBytes / (1024. * 1024. * 1024.),
                 wroteBytes / (1024. * 1024. * 1024.));
          nprint = std::min(nprint << 1, 10000u);
          toprint = nprint;
        }
      }
    } catch (const std::exception &e) {
      printf("%02u: Terminating an exception was raised:\n%s\n", id_, e.what());
      ret = 1;
    }
    if (fout_.is_open()) {
      assert(fname_.length() > 4);
      rename(fname_.c_str(), fname_.substr(0, fname_.length() - 4).c_str());
      printf("Moved %s -> %s\n", fname_.c_str(), fname_.substr(0, fname_.length() - 4).c_str());
      fout_.close();
    }
    auto tend = std::chrono::steady_clock::now();
    std::chrono::duration<double> dt = tend - tstart;
    double readRate = readBytes / 1024.0 / 1024.0 / 1024.0 / dt.count();
    double wroteRate = wroteBytes / 1024.0 / 1024.0 / 1024.0 / dt.count();
    printf("%02u: Read %u orbits (%u truncated, %.4f%%), %.3f GB\n",
           id_,
           orbits_.load(),
           truncorbits_.load(),
           truncorbits_.load() * 100.0 / orbits_.load(),
           readBytes / (1024. * 1024. * 1024.));
    printf("%02u: Wrote %.3f GB to %s\n", id_, wroteBytes / (1024. * 1024. * 1024.), fname_.c_str());
    printf("%02u: Data rate in %.3f GB/s, out %.3f GB/s\n", id_, readRate, wroteRate);
    printf("\n");
    std::free(orbit_buff);
    return ret;
  }

protected:
  unsigned int orbitsPerFile_, prescale_;
  std::string basepath_, fname_;
  std::fstream fout_;
  bool checkData_;
};

int setup_tcp(const char *addr, const char *port, unsigned int port_offs = 0) {
  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    perror("ERROR opening socket");
    return -1;
  }
  struct sockaddr_in serv_addr;
  bzero((char *)&serv_addr, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(std::atoi(port) + port_offs);
  serv_addr.sin_addr.s_addr = inet_addr(addr);
  if (bind(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    perror("ERROR on binding");
    return -2;
  }
  printf("Set up server on %s port %u\n", addr, std::atoi(port) + port_offs);
  fflush(stdout);
  listen(sockfd, 5);
  return sockfd;
}

int receive_tcp(const char *addr, const char *port, unsigned int port_offs = 0) {
  thread_local static int sockfd = setup_tcp(addr, port, port_offs);  // done only once per job
  struct sockaddr_in cli_addr;
  bzero((char *)&cli_addr, sizeof(cli_addr));
  socklen_t clilen = sizeof(cli_addr);
  printf("Wait for client on %s port %u\n", addr, std::atoi(port) + port_offs);
  int newsockfd = accept(sockfd, (struct sockaddr *)&cli_addr, &clilen);
  if (newsockfd < 0) {
    perror("ERROR on accept");
    return -3;
  }
  printf("Connection accepted on port %u\n", std::atoi(port) + port_offs);
  fflush(stdout);
  return newsockfd;
}

int open_file(const char *fname) {
  int sourcefd = open(fname, O_NOATIME | O_RDONLY);
  if (sourcefd < 0) {
    printf("Can't open file %s\n", fname);
  }
  return sourcefd;
}

int open_source(const std::string &src, unsigned int iclient = 0) {
  auto pos = src.find(':');
  if (pos == std::string::npos) {
    pos = src.find("%d");
    if (pos == std::string::npos) {
      assert(iclient == 0);
      return open_file(src.c_str());
    } else {
      std::string myfile = src.substr(0, pos) + std::to_string(iclient) + src.substr(pos + 2);
      return open_file(myfile.c_str());
    }
  } else {
    std::string ip = src.substr(0, pos), port = src.substr(pos + 1);
    return receive_tcp(ip.c_str(), port.c_str(), iclient);
  }
}
class TrashData : public CheckerBase {
public:
  TrashData(unsigned int buffSize_kb) : CheckerBase(), buffSize_(buffSize_kb * 1024) {}

  int run(int sockfd) override {
    char *buff = reinterpret_cast<char *>(std::aligned_alloc(4096u, buffSize_));
    auto tstart = std::chrono::steady_clock::now();
    unsigned long data = 0, calls = 0;
    int n;
    do {
      if (calls == 0)
        tstart = std::chrono::steady_clock::now();
      n = read(sockfd, buff, buffSize_);
      calls++;
      data += n;
    } while (n > 0);
    auto tend = std::chrono::steady_clock::now();
    std::chrono::duration<double> dt = tend - tstart;
    double dataGB = data / 1024.0 / 1024.0 / 1024.0;
    double datarate = dataGB / dt.count();
    printf("Read for %.2f ms, with %lu calls, received %.2f GB\n", dt.count() * 1000, calls, dataGB);
    printf("Data rate %.2f GB/s, %.1f Gbps. Call rate %.1f kHz\n", datarate, datarate * 8, calls / dt.count() / 1000.);
    return 0;
  }

protected:
  const unsigned int buffSize_;
};

class ReceiveAndStore : public CheckerBase {
public:
  ReceiveAndStore(unsigned int readSize_kb, unsigned int buffSize_kb, double fileSize_gb, const char *fname)
      : CheckerBase(),
        readSize_(readSize_kb * 1024),
        buffSize_(buffSize_kb * 1024),
        nbuffs_(std::ceil(fileSize_gb * 1024 / (buffSize_kb / 1024))),
        buffs_(nbuffs_),
        sizes_(nbuffs_, -1),
        fname_(fname) {
    for (unsigned int i = 0; i < nbuffs_; ++i) {
      buffs_[i] = reinterpret_cast<char *>(std::aligned_alloc(4096u, buffSize_));
      buffs_[i][0] = '\0';  // touch the memory
    }
    printf("pre-allocated %u buffers\n", nbuffs_);
  }

  int run(int sockfd) override {
    const unsigned int max_size = buffSize_ - readSize_;
    auto tstart = std::chrono::steady_clock::now();
    unsigned long data = 0, calls = 0;
    int n;
    for (unsigned int i = 0; i < nbuffs_; ++i) {
      char *ptr = buffs_[i];
      unsigned int size = 0;
      do {
        n = read(sockfd, ptr, readSize_);
        if (n <= 0)
          break;
        calls++;
        if (i == 0)
          tstart = std::chrono::steady_clock::now();
        size += n;
        ptr += n;
      } while (size < max_size);
      sizes_[i] = size;
      data += size;
    }
    auto tend = std::chrono::steady_clock::now();
    std::chrono::duration<double> dt = tend - tstart;
    double dataGB = data / 1024.0 / 1024.0 / 1024.0;
    double datarate = dataGB / dt.count();
    printf("Read for %.2f ms, with %lu calls, received %.2f GB\n", dt.count() * 1000, calls, dataGB);
    printf("Data rate %.2f GB/s, %.1f Gbps. Call rate %.1f kHz\n", datarate, datarate * 8, calls / dt.count() / 1000.);
    std::fstream fout(fname_.c_str(), std::ios_base::binary | std::ios_base::out | std::ios_base::trunc);
    for (unsigned int i = 0; i < nbuffs_; ++i) {
      if (sizes_[i] <= 0)
        break;
      fout.write(buffs_[i], sizes_[i]);
    }
    auto tflush = std::chrono::steady_clock::now();
    dt = tflush - tend;
    printf("Flushed to %s in %.2f ms (%.2f GB/s, %.1f Gbps)\n",
           fname_.c_str(),
           dt.count() * 1000,
           dataGB / dt.count(),
           8 * dataGB / dt.count());
    return 0;
  }

protected:
  unsigned int readSize_, buffSize_, nbuffs_;
  std::vector<char *> buffs_;
  std::vector<int> sizes_;
  std::string fname_;
};

int print_usage(const char *self, int retval) {
  printf("Usage: %s  [options] Algo ( file | ip:port )\n", self);
  printf("Algo: DTHBasic, DTHBasicOA, DTHBasicOA-NC, TCP-trash, TCP-store\n");
  printf("   -d, --debug N  : print out the first N events\n");
  printf("   -T, --tmux  T  : runs at TMUX T (default: 6)\n");
  printf("    --orbitmux N  : mux orbits by factor N (default: 1)\n");
  printf("    --maxorbits N : stop after this number of orbits (default: 10000000)\n");
  printf("   -t, --tslice T : runs tslice t  (default: 0)\n");
  printf("   -B, --buffsize B : uses a read buffer size of B kB  (default: 4)\n");
  printf("   -O  --orbsize  B : uses an orbit buffer size of B kB (default: 2048)\n");
  printf("   -k  --keep    : keep running \n");
  printf("   -n, --nclients N : runs N clients for timelices 0..N-1 with increasing port numbers\n");
  printf("   -p, --prescale N : prescale output by a factor N (save orbit %% prescale == 1)\n");
  printf("\n");
  return retval;
}

void start_and_run(std::unique_ptr<CheckerBase> &&checker,
                   const std::string &src,
                   int client,
                   bool keep_running,
                   std::atomic<unsigned int> *errors) {
  printf("Starting in client %d\n", client);
  do {
    int sourcefd = open_source(src, client);
    if (sourcefd < 0) {
      printf("Error in opening source %s for client %d.\n", src.c_str(), client);
      return;
    }
    int ret = checker->run(sourcefd);
    if (ret) {
      errors->fetch_add(1);
      if (!keep_running) _exit(1);
    }
    checker->clear();
  } while (keep_running);
  printf("Done in client %d\n", client);
}

int main(int argc, char **argv) {
  int debug = 0, tmux = 6, tmux_slice = 0, orbitmux = 1, buffsize_kb = 4, orbsize_kb = 2048, nclients = 1, prescale = 1;
  unsigned int maxorbits = 10000000;
  bool keep_running = false, zeropad = false;
  while (1) {
    static struct option long_options[] = {{"help", no_argument, nullptr, 'h'},
                                           {"keep", no_argument, nullptr, 'k'},
                                           {"debug", required_argument, nullptr, 'd'},
                                           {"tmux", required_argument, nullptr, 'T'},
                                           {"orbitmux", required_argument, nullptr, 1},
                                           {"maxorbits", required_argument, nullptr, 2},
                                           {"tslice", required_argument, nullptr, 't'},
                                           {"buffsize", required_argument, nullptr, 'B'},
                                           {"orbsize", required_argument, nullptr, 'O'},
                                           {"nclients", required_argument, nullptr, 'n'},
                                           {"prescale", required_argument, nullptr, 'p'},
                                           {"zeropad", no_argument, nullptr, 'z'},
                                           {nullptr, 0, nullptr, 0}};
    /* getopt_long stores the option index here. */
    int option_index = 0;
    int optc = getopt_long(argc, argv, "khd:T:t:B:O:n:p:z", long_options, &option_index);

    /* Detect the end of the options. */
    if (optc == -1)
      break;

    switch (optc) {
      case 'h':
        return print_usage(argv[0], 0);
      case 'd':
        debug = std::atoi(optarg);
        break;
      case 't':
        tmux_slice = std::atoi(optarg);
        break;
      case 'n':
        nclients = std::atoi(optarg);
        break;
      case 'T':
        tmux = std::atoi(optarg);
        break;
      case 1:
        orbitmux = std::atoi(optarg);
        break;
      case 2:
        maxorbits = std::atol(optarg);
        break;
      case 'B':
        buffsize_kb = std::atoi(optarg);
        break;
      case 'O':
        orbsize_kb = std::atoi(optarg);
        break;
      case 'p':
        prescale = std::atoi(optarg);
        break;
      case 'k':
        keep_running = true;
        break;
      case 'z':
        zeropad = true;
        break;
      default:
        return print_usage(argv[0], 1);
    }
  }

  int nargs = argc - optind;
  if (nargs < 2)
    return print_usage(argv[0], 1);
  std::string kind(argv[optind++]);
  std::string src(argv[optind++]);

  int ret = 0;
  std::atomic<unsigned int> client_errors = 0;
  std::vector<std::thread> client_threads;
  for (int client = 0; client < nclients; ++client) {
    std::unique_ptr<CheckerBase> checker;
    if (kind == "Native128") {
      checker.reset(new NativeChecker(/*padTo128=*/true));
    } else if (kind == "Native64") {
      checker.reset(new NativeChecker(/*padTo128=*/false));
    } else if (kind == "Native64SZ") {
      checker.reset(new NativeChecker(/*padTo128=*/false, /*skipZeros=*/true));
    } else if (kind == "DTHBasic") {
      checker.reset(new DTHBasicChecker());
    } else if (kind == "DTHBasicOA" || kind == "DTHBasicOA-NC" || kind == "DTHBasicOA-NoSR") {
      checker.reset(new DTHBasicCheckerOA(orbsize_kb, kind != "DTHBasicOA-NC", kind != "DTHBasicOA-NoSR"));
    } else if (kind == "DTHReceiveOA") {
      if (nargs != 3) {
        printf("Usage: %s DTHReceiveOA ip:port outfile\n", argv[0]);
        return 3;
      }
      checker.reset(new DTHReceiveOA(orbsize_kb, argv[optind], prescale));
    } else if (kind == "DTHBasic256" || kind == "DTHBasic256-NC") {
      checker.reset(new DTHBasicChecker256(orbsize_kb, kind != "DTHBasic256-NC", zeropad));
    } else if (kind == "DTHReceive256") {
      if (nargs != 3) {
        printf("Usage: %s DTHReceive256 ip:port outfile\n", argv[0]);
        return 3;
      }
      checker.reset(new DTHReceive256(orbsize_kb, argv[optind], prescale));
    } else if (kind == "DTHRollingReceive256") {
      if (nargs != 4) {
        printf("Usage: %s DTHRollingReceive256 ip:port outfile orbitsPerFile \n", argv[0]);
        return 3;
      }
      checker.reset(new DTHRollingReceive256(orbsize_kb, argv[optind], std::atoi(argv[optind + 1]), prescale));
    } else if (kind == "TrashData") {
      checker.reset(new TrashData(buffsize_kb));
    } else if (kind == "ReceiveAndStore") {
      if (nargs != 4) {
        printf("Usage: %s ReceiveAndStore ip:port filesize_Gb outfile\n", argv[0]);
        return 3;
      }
      checker.reset(new ReceiveAndStore(buffsize_kb, orbsize_kb, std::atof(argv[optind]), argv[optind + 1]));
    } else {
      printf("Unsupported mode '%s'\n", kind.c_str());
      return 3;
    }
    if (orbitmux == 1) {
      checker->init(tmux, tmux_slice + client, orbitmux, maxorbits);
    } else {
      checker->init(tmux, tmux_slice, orbitmux, maxorbits);
      checker->setId(client);
    }
    checker->setDebug(debug);

    client_threads.emplace_back(start_and_run, std::move(checker), src, client, keep_running, &client_errors);
  }
  for (auto &t : client_threads)
    t.join();
  if (client_errors > 0)
    ret = 1;
  return ret;
}