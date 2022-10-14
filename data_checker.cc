#include <cstdio>
#include <cstdint>
#include <fstream>
#include <string>
#include <chrono>
#include <cstdlib>
#include <cassert>
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
    events_ = 0;
    orbits_ = 0;
    truncevents_ = 0;
    truncevents_orbit_ = 0;
    truncorbits_ = 0;
    maxorbits_ = maxorbits;
    puppis_ = 0;
    tmux_ = tmux;
    firstbx_ = firstBx;
    orbitMux_ = orbitMux;
    oldbx_ = std::numeric_limits<unsigned int>::max();
    oldorbit_ = std::numeric_limits<unsigned int>::max();
    oldrecord_ = std::numeric_limits<uint64_t>::max();
    ndebug_ = 0;
  }
  void setDebug(int ndebug = 5) { ndebug_ = ndebug; }

  static void print128(const uint8_t *start) {
    for (unsigned int i = 0; i < 16; ++i)
      printf("%02x ", unsigned(start[i]));
  }

  struct DTH_Header {
    uint8_t size128;
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
      throw std::runtime_error("Record number mismatch, found " + std::to_string(evno) + " after " +
                               std::to_string(oldrecord_));
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
  };

  PuppiHeader readPuppiHeader(int sockfd, uint64_t &out) {
    int n = read(sockfd, reinterpret_cast<char *>(&out), 8);
    if (n == 0) {
      out = 0;
      return PuppiHeader();
    }
    if (n != 8)
      throw std::runtime_error("Failed reading Event Header, got only " + std::to_string(n) + "/8 bytes.");
    return parsePuppiHeader(out);
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
    if ((out >> 62) != 0b10)
      throw std::runtime_error("Bad event header found: " + std::to_string(out));
    if (events_ < ndebug_) {
      printf("Event header %016lx, orbit %u (%x), bx %u, npuppi %d\n", out, ret.orbit, ret.orbit, ret.bx, ret.npuppi);
    }
    return ret;
  }

  void countEventsAndOrbits(unsigned long orbit, unsigned int bx, bool truncated = false) {
    if (orbit != oldorbit_) {
      if (oldorbit_ != std::numeric_limits<unsigned int>::max() && (orbit != oldorbit_ + orbitMux_)) {
        throw std::runtime_error("Orbit mismatch: found orbit " + std::to_string(orbit) + " bx " + std::to_string(bx) +
                                 " after orbit " + std::to_string(oldorbit_) + " bx " + std::to_string(oldbx_));
      }
      if (truncevents_orbit_ > 0)
        truncorbits_++;
      truncevents_orbit_ = 0;
      orbits_++;
      oldorbit_ = orbit;
      if (oldbx_ != std::numeric_limits<unsigned int>::max() && bx != firstbx_) {
        throw std::runtime_error("BX mismatch: found " + std::to_string(unsigned(bx)) +
                                 " at beginning of orbit, instead of " + std::to_string(firstbx_));
      }
      oldbx_ = bx;
    } else {
      if (oldbx_ != std::numeric_limits<unsigned int>::max() && bx != oldbx_ + tmux_) {
        throw std::runtime_error("BX mismatch: found " + std::to_string(unsigned(bx)) + " after " +
                                 std::to_string(oldbx_) + ", expected " + std::to_string(oldbx_ + tmux_));
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
    printf("Read %u orbits, %lu events, %lu candidates, %lu bytes\n",
           orbits_.load(),
           events_.load(),
           puppis_.load(),
           totbytes);
    printf("Time to read %.2f ms, LHC time %.2f ms (x %.3f)\n", dt.count() * 1000, dt_lhc * 1000, dt_lhc / dt.count());
    printf("Orbit rate %.3f kHz (TM1 %.3f kHz), event rate %.2f kHz\n",
           orbrate / 1000,
           orbrate_lhc / 1000.,
           evrate / 1000.);
    printf("Data rate %.3f GB/s, input data rate %.3f GB/s (x %.3f)\n", datarate, fillrate, fillrate / datarate);
    if (truncevents_ || truncorbits_) {
      printf("Truncated events %lu (%.4f%%), orbits %u (%.4f%%)\n",
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
  unsigned int firstbx_, tmux_, orbitMux_;
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
  NativeChecker(bool padTo128 = true) : CheckerBase(), padTo128_(padTo128) {}
  ~NativeChecker() override{};

  int run(int in) override {
    uint64_t buff64;
    unsigned int nprint = 100;
    auto tstart = std::chrono::steady_clock::now();
    try {
      while (good(in) && orbits_ <= maxorbits_) {
        PuppiHeader evh = readPuppiHeader(in, buff64);
        if (buff64 == 0) break;
        unsigned int n64 = evh.npuppi + 1;
        bool truncated = false;
        if (padTo128_) {
          n64 += n64 % 2;
          if (evh.npuppi == 0) {
            int n = read(in, reinterpret_cast<char *>(&buff64), 8);
            assert(n == 8);
            truncated = (buff64 != 0);
          }
        }
        puppis_ += evh.npuppi;
        if (!truncated)
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
    }
    auto tend = std::chrono::steady_clock::now();
    printDone(tstart, tend);
    return 0;
  }

protected:
  bool padTo128_;
};

class DTHBasicChecker : public CheckerBase {
public:
  DTHBasicChecker() : CheckerBase() {}
  ~DTHBasicChecker() override{};

  int run(int in) override {
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
    }
    auto tend = std::chrono::steady_clock::now();
    printDone(tstart, tend);
    return 0;
  }
};

class DTHBasicCheckerOA : public CheckerBase {
public:
  DTHBasicCheckerOA(unsigned int orbSize_kb, bool checkData = true)
      : CheckerBase(), orbSize_(orbSize_kb * 1024), checkData_(checkData) {}
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
          if (totlen128 == 2)
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
        uint64_t orbitno = readSRHeader(ptr, end, false);
        if (orbitno == 0)
          break;
        if (totlen128 == 2) {  // truncated orbit
          SR_Trailer srt = readSRTrailer(ptr, end);
          assert(totlen128 == srt.len);
          assert(oldorbit_ == std::numeric_limits<unsigned int>::max() || srt.orbit == oldorbit_ + 1);
          oldorbit_ = srt.orbit;
          orbits_ += 1;
          truncorbits_ += 1;
          continue;
        }
        while (ptr + 16 < end) {
          PuppiHeader evh = readPuppiHeader(ptr, end, buff64);
          unsigned int n64 = evh.npuppi + 1;
          n64 += n64 & 1;
          puppis_ += evh.npuppi;
          bool truncated = (evh.npuppi == 0) && (*ptr != 0);
          ptr += (n64 - 1) << 3;
          countEventsAndOrbits(evh.orbit, evh.bx, truncated);
        }
        SR_Trailer srt = readSRTrailer(ptr, end);
        assert(totlen128 == srt.len);
        assert(oldorbit_ == std::numeric_limits<unsigned int>::max() || srt.orbit == oldorbit_);
        if (--toprint == 0) {
          if (truncevents_) {
            printf("Read %10lu events, %7u orbits. Truncated %8lu events, %7u orbits\n",
                   events_.load(),
                   orbits_.load(),
                   truncevents_.load(),
                   truncorbits_.load());
          } else {
            printf("Read %10lu events, %7u orbits\n", events_.load(), orbits_.load());
          }
          nprint = std::min(nprint << 1, 10000u);
          toprint = nprint;
        }
      }
    } catch (const std::exception &e) {
      printf("Terminating an exception was raised:\n%s\n", e.what());
    }
    auto tend = std::chrono::steady_clock::now();
    printDone(tstart, tend);
    std::free(orbit_buff);
    return 0;
  }

protected:
  unsigned int orbSize_;
  bool checkData_;
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
        if (totlen128 == 2)
          truncorbits_++;
        readBytes += totlen128 << 4;
        if (orbits_ % prescale_ == 0) {
          fout_.write(reinterpret_cast<char *>(orbit_buff+16), (totlen128-2) << 4);
          wroteBytes += totlen128 << 4;
        }
        if (--toprint == 0) {
          printf("Read %7u orbits (%7u truncated, %.4f%%), %9.3f GB. Wrote %6.3f GB.\n",
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
      printf("Terminating an exception was raised:\n%s\n", e.what());
    }
    auto tend = std::chrono::steady_clock::now();
    std::chrono::duration<double> dt = tend - tstart;
    double readRate = readBytes / 1024.0 / 1024.0 / 1024.0 / dt.count();
    double wroteRate = wroteBytes / 1024.0 / 1024.0 / 1024.0 / dt.count();
    printf("Read %u orbits (%u truncated, %.4f%%), %.3f GB\n",
           orbits_.load(),
           truncorbits_.load(),
           truncorbits_.load() * 100.0 / orbits_.load(),
           readBytes / (1024. * 1024. * 1024.));
    printf("Wrote %.3f GB to %s\n",
           wroteBytes / (1024. * 1024. * 1024.),
           fname_.c_str());
    printf("Data rate in %.3f GB/s, out %.3f GB/s\n", readRate, wroteRate);
    printf("\n");
    std::free(orbit_buff);
    return 0;
  }

protected:
  unsigned int prescale_;
  unsigned long int maxSize_;
  std::string fname_;
  std::fstream fout_;
  bool checkData_;
};

int setup_tcp(const char *addr, const char *port) {
  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    perror("ERROR opening socket");
    return -1;
  }
  struct sockaddr_in serv_addr;
  bzero((char *)&serv_addr, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(atoi(port));
  serv_addr.sin_addr.s_addr = inet_addr(addr);
  if (bind(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    perror("ERROR on binding");
    return -2;
  }
  printf("Set up server on %s port %s\n", addr, port);
  fflush(stdout);
  listen(sockfd, 5);
  return sockfd;
}

int receive_tcp(const char *addr, const char *port) {
  static int sockfd = setup_tcp(addr, port);  // done only once per job
  struct sockaddr_in cli_addr;
  bzero((char *)&cli_addr, sizeof(cli_addr));
  socklen_t clilen = sizeof(cli_addr);
  printf("Wait for client on %s port %s\n", addr, port);
  int newsockfd = accept(sockfd, (struct sockaddr *)&cli_addr, &clilen);
  if (newsockfd < 0) {
    perror("ERROR on accept");
    return -3;
  }
  printf("Connection accepted\n");
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

int open_source(const std::string &src) {
  auto pos = src.find(':');
  if (pos == std::string::npos) {
    return open_file(src.c_str());
  } else {
    std::string ip = src.substr(0, pos), port = src.substr(pos + 1);
    return receive_tcp(ip.c_str(), port.c_str());
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
  ReceiveAndStore(unsigned int readSize_kb, unsigned int buffSize_kb, unsigned int fileSize_gb, const char *fname)
      : CheckerBase(),
        readSize_(readSize_kb * 1024),
        buffSize_(buffSize_kb * 1024),
        nbuffs_(fileSize_gb * 1024 / (buffSize_kb / 1024)),
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
  printf("   -t, --tslice T : runs tslice t  (default: 0)\n");
  printf("   -B, --buffsize B : uses a read buffer size of B kB  (default: 4)\n");
  printf("   -O  --orbsize  B : uses an orbit buffer size of B kB (default: 2048)\n");
  printf("   -k  --keep    : keep running \n");
  printf("\n");
  return retval;
}
int main(int argc, char **argv) {
  int debug = 0, tmux = 6, tmux_slice = 0, orbitmux = 1, buffsize_kb = 4, orbsize_kb = 2048;
  bool keep_running = 0;
  while (1) {
    static struct option long_options[] = {{"help", no_argument, nullptr, 'h'},
                                           {"keep", no_argument, nullptr, 'k'},
                                           {"debug", required_argument, nullptr, 'd'},
                                           {"tmux", required_argument, nullptr, 'T'},
                                           {"orbitmux", required_argument, nullptr, 1},
                                           {"tslice", required_argument, nullptr, 't'},
                                           {"buffsize", required_argument, nullptr, 'B'},
                                           {"orbsize", required_argument, nullptr, 'O'},
                                           {nullptr, 0, nullptr, 0}};
    /* getopt_long stores the option index here. */
    int option_index = 0;
    int optc = getopt_long(argc, argv, "khd:T:t:b:O:", long_options, &option_index);

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
      case 'T':
        tmux = std::atoi(optarg);
        break;
      case 1:
        orbitmux = std::atoi(optarg);
        break;
      case 'B':
        buffsize_kb = std::atoi(optarg);
        break;
      case 'O':
        orbsize_kb = std::atoi(optarg);
        break;
      case 'k':
        keep_running = true;
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

  int ret;
  do {
    std::unique_ptr<CheckerBase> checker;
    if (kind == "Native128") {
      checker.reset(new NativeChecker(/*padTo128=*/true));
    } else if (kind == "DTHBasic") {
      checker.reset(new DTHBasicChecker());
    } else if (kind == "DTHBasicOA" || kind == "DTHBasicOA-NC") {
      checker.reset(new DTHBasicCheckerOA(orbsize_kb, kind != "DTHBasicOA-NC"));
    } else if (kind == "DTHReceiveOA") {
      if (nargs != 3 && nargs != 4) {
        printf("Usage: %s DTHReceiveOA ip:port outfile [prescale] \n", argv[0]);
        return 3;
      }
      unsigned int prescale = (nargs == 3) ? 100 : std::atoi(argv[optind+1]);
      checker.reset(new DTHReceiveOA(orbsize_kb, argv[optind], prescale));
    } else if (kind == "TrashData") {
      checker.reset(new TrashData(buffsize_kb));
    } else if (kind == "ReceiveAndStore") {
      if (nargs != 4) {
        printf("Usage: %s ReceiveAndStore ip:port filesize_Gb outfile\n", argv[0]);
        return 3;
      }
      checker.reset(new ReceiveAndStore(buffsize_kb, orbsize_kb, std::atoi(argv[optind]), argv[optind + 1]));
    } else {
      printf("Unsupported mode '%s'\n", kind.c_str());
      return 3;
    }
    checker->init(tmux, tmux_slice, orbitmux);
    checker->setDebug(debug);

    int sourcefd = open_source(src);
    if (sourcefd < 0)
      return 2;

    ret = checker->run(sourcefd);
  } while (keep_running);
  return ret;
}