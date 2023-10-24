#include <cstdio>
#include <cstdint>
#include <fstream>
#include <string>
#include <chrono>
#include <cstdlib>
#include <cassert>
#include <exception>
#include <vector>
#include <array>

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

#include <iostream>
#include <random>
#include <algorithm>
#include <getopt.h>

#include <sys/uio.h>

typedef std::chrono::time_point<std::chrono::steady_clock> tp;

class ResamplerData {
public:
  ResamplerData(const std::string &fileName) {
    std::fstream fin(fileName, std::ios_base::in | std::ios_base::binary);
    uint64_t header, data[255];
    uint16_t nwords;
    while (fin.good()) {
      fin.read(reinterpret_cast<char *>(&header), sizeof(uint64_t));
      nwords = (header & 0xFFF);
      if (nwords)
        fin.read(reinterpret_cast<char *>(data), nwords * sizeof(uint64_t));
      events.emplace_back(std::min<uint16_t>(nwords, 207), data);
    }
    std::cout << "ResamplerData created with " << events.size() << " templates" << std::endl;
  }
  struct TemplateEvent {
    uint16_t size;
    std::vector<uint64_t> payload;
    TemplateEvent(uint16_t nwords, const uint64_t *data) : size(nwords) {
      payload.insert(payload.end(), data, data + nwords);
    }
  };

  std::vector<TemplateEvent> events;
};

class OrbitPayloadResampler {
public:
  const unsigned int NBX = 3564;
  OrbitPayloadResampler(const ResamplerData &data, unsigned int tmux, unsigned int offs, unsigned int seed = 37)
      : data_(&data), tmux_(tmux), offs_(offs), rnd_(seed) {
    std::cout << "OrbitPayloadResampler created with " << data.events.size() << " templates and seed " << seed
              << std::endl;
  }
  unsigned int fillOrbit(uint32_t orbitno, uint64_t *begin, uint64_t *end) {
    uint64_t *ptr = begin;
    unsigned int nevents = data_->events.size();
    if (offs_ > 0)
      rnd_.discard(offs_ - 1);
    for (unsigned int bx = offs_; bx < NBX; bx += tmux_) {
      unsigned int irnd = rnd_();
      //if (tmux_ != 1) rnd_.discard(tmux_-1); // align random numbers (but increase CPU usage)
      const auto &item = data_->events[irnd % nevents];
      (*ptr) = (0b10llu << 62) | (uint64_t(orbitno) << 24) | (bx << 12) | item.size;
      assert(ptr + item.size + 1 < end);
      ptr = std::copy_n(item.payload.begin(), item.size, ptr + 1);
    }
    return (ptr - begin);
  }

private:
  const ResamplerData *data_;
  unsigned int tmux_, offs_;
  std::ranlux48_base rnd_;
};

class GeneratorBase {
public:
  GeneratorBase(const ResamplerData &data, unsigned int tmux, unsigned int offs, unsigned int seed = 37)
      : src_(data, tmux, offs, seed) {}
  virtual ~GeneratorBase() {}
  virtual void generate(int fd, unsigned int norbits, unsigned int firstOrbit, unsigned int orbitmux) = 0;

protected:
  OrbitPayloadResampler src_;
};

class Native64Generator : public GeneratorBase {
public:
  Native64Generator(const ResamplerData &data,
                    unsigned int tmux,
                    unsigned int offs,
                    unsigned int seed,
                    unsigned int orbitSize = 2 * 1024 * 1024)
      : GeneratorBase(data, tmux, offs, seed), orbSize_(orbitSize) {}
  void generate(int fd, unsigned int norbits, unsigned int firstOrbit, unsigned int orbitmux) override final {
    uint64_t *orbit_buff = reinterpret_cast<uint64_t *>(std::aligned_alloc(4096u, orbSize_));
    uint64_t *end_buff = orbit_buff + (orbSize_ / sizeof(uint64_t));
    for (unsigned int i = firstOrbit, lastOrbit = firstOrbit + norbits; i < lastOrbit; i += orbitmux) {
      unsigned int nwords = src_.fillOrbit(i, orbit_buff, end_buff);
      write(fd, orbit_buff, nwords * sizeof(uint64_t));
    }
    std::free(orbit_buff);
  }

private:
  unsigned int orbSize_;
};

class DTH256Generator : public GeneratorBase {
public:
  DTH256Generator(const ResamplerData &data,
                  unsigned int tmux,
                  unsigned int offs,
                  unsigned int seed,
                  bool sync,
                  unsigned int orbitSize = 2 * 1024 * 1024)
      : GeneratorBase(data, tmux, offs, seed), orbSize_(orbitSize), sync_(sync) {}
  void generate(int fd, unsigned int norbits, unsigned int firstOrbit, unsigned int orbitmux) override final {
    constexpr unsigned int packetSize = 256 * 256 / 8;  // max DTH256 packet size is 256 rows of 256 bits
    unsigned int maxPackets = std::ceil(float(orbSize_) / packetSize);
    std::vector<struct iovec> iovecs(2 * maxPackets);
    std::vector<std::array<uint8_t, 32>> headers(maxPackets);
    for (unsigned int i = 0; i < maxPackets; ++i) {
      auto &header = headers[i];
      std::fill(header.begin(), header.end(), 0);
      header[0] = 0x47;
      header[1] = 0x5a;
      iovecs[2 * i].iov_base = reinterpret_cast<void *>(&headers[i].front());
      iovecs[2 * i].iov_len = 32;
    }

    uint64_t *orbit_buff = reinterpret_cast<uint64_t *>(std::aligned_alloc(4096u, orbSize_));
    uint64_t *end_buff = orbit_buff + (orbSize_ / sizeof(uint64_t));

    double orbitTime = 3564 / 40e6;
    auto tstart = std::chrono::steady_clock::now();
    for (unsigned int i = firstOrbit, lastOrbit = firstOrbit + norbits; i < lastOrbit; i += orbitmux) {
      unsigned int nwords = src_.fillOrbit(i, orbit_buff, end_buff);
      bool first = true;
      uint64_t *ptr = orbit_buff;
      // add 3 null words for safety
      for (int w = 0; w < 3; ++w)
        orbit_buff[nwords + w] = 0;
      unsigned int ipacket = 0;
      for (int n256 = (nwords + 3) >> 2; n256 > 0; n256 -= 255, ++ipacket) {
        unsigned int chunksize256 = std::min<int>(n256, 255);
        auto &header = headers[ipacket];
        auto &iov_data = iovecs[2 * ipacket + 1];
        header[6] = chunksize256;
        header[5] = (first ? (1 << 7) : 0) | (n256 <= 255 ? (1 << 6) : 0);
        iov_data.iov_base = ptr;
        iov_data.iov_len = (chunksize256 << 2) * sizeof(uint64_t);
        ptr += (chunksize256 << 2);
        first = false;
      }
      writev(fd, &iovecs.front(), 2 * ipacket);
      if (sync_) {
        auto tend = std::chrono::steady_clock::now();
        double dt = (std::chrono::duration<double>(tend - tstart)).count();
        if (dt < orbitTime * i) {
          std::this_thread::sleep_for(std::chrono::duration<double>(orbitTime * i - dt));
        }
        if (i % 5000 == 0) {
          double orbrate = i / orbitmux / dt, orbrate_lhc = 40e6 / 3564;
          printf("Generator running, wrote %u orbits in %.2f ms (%.3f kHz, x %.3f)\n",
                 i / orbitmux,
                 dt * 1000,
                 orbrate / 1000,
                 orbrate / orbrate_lhc);
        }
      }
    }
    std::free(orbit_buff);
  }

private:
  unsigned int orbSize_;
  bool sync_;
};

class CMSSWGenerator : public GeneratorBase {
public:
  CMSSWGenerator(const ResamplerData &data,
                 unsigned int tmux,
                 unsigned int offs,
                 unsigned int seed,
                 unsigned int run,
                 unsigned int lumisection,
                 unsigned int orbitSize = 2 * 1024 * 1024)
      : GeneratorBase(data, tmux, offs, seed), run_(run), lumisection_(lumisection), orbSize_(orbitSize) {}
  void generate(int fd, unsigned int norbits, unsigned int firstOrbit, unsigned int orbitmux) override final {
    uint64_t *orbit_buff = reinterpret_cast<uint64_t *>(std::aligned_alloc(4096u, orbSize_));
    uint64_t *end_buff = orbit_buff + (orbSize_ / sizeof(uint64_t));
    uint64_t total_size = 32;
    // insert a dummy event payload first
    uint8_t fileHeader[32];
    std::fill(fileHeader, fileHeader + 32, 0);
    std::copy_n("RAW_0002", 8, fileHeader);
    *reinterpret_cast<uint16_t *>(&fileHeader[8]) = 32;
    *reinterpret_cast<uint16_t *>(&fileHeader[10]) = 20;
    *reinterpret_cast<uint32_t *>(&fileHeader[12]) = norbits;
    *reinterpret_cast<uint32_t *>(&fileHeader[16]) = run_;
    *reinterpret_cast<uint32_t *>(&fileHeader[20]) = lumisection_;
    write(fd, fileHeader, 32);
    uint32_t orbitHeader[6] = {6, run_, lumisection_, 0, 0, 0};
    for (unsigned int orbitno = firstOrbit, lastOrbit = firstOrbit + norbits; orbitno < lastOrbit;
         orbitno += orbitmux) {
      unsigned int nwords = src_.fillOrbit(orbitno, orbit_buff, end_buff);
      orbitHeader[3] = orbitno;
      orbitHeader[4] = nwords << 3;
      write(fd, orbitHeader, 24);
      write(fd, orbit_buff, nwords << 3);
      total_size += (nwords << 3) + 24;
    }
    lseek(fd, 24, SEEK_SET);
    write(fd, &total_size, 8);
    lseek(fd, 0, SEEK_END);
    std::free(orbit_buff);
  }

private:
  unsigned int run_, lumisection_, orbSize_;
};

int connect_tcp(const char *addr, const char *port, unsigned int port_offs = 0) {
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
  if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr))) {
    perror("ERROR on connect");
    return -2;
  }
  std::cout << "Connected to server on " << addr << ", port " << (std::atoi(port) + port_offs) << std::endl;
  return sockfd;
}

int print_usage(const char *self, int retval) {
  printf("Usage: %s  [options] Algo srcFile ( file | ip:port )\n", self);
  printf("    --orbits N  : generate N orbits (default: 10)\n");
  printf("    --time   [ <N>s | <N>m | N<h> ]  : run for the specified amount of time in seconds, minutes, hours\n");
  printf("   -T, --tmux  T  : runs at TMUX T (default: 6)\n");
  printf("    --orbitmux N  : mux orbits by factor N (default: 1)\n");
  printf("   -t, --tslice T : runs tslice t  (default: 0)\n");
  printf("   -r, --run   N : sets CMSSW run number\n");
  printf("   -l, --ls    N : sets CMSSW lumisection number \n");
  printf("   --orbitsPerLumi : sets number of orbits per lumisection (default: 1024) \n");
  printf("   -s, --seed   N : sets seed of RNG\n");
  printf("   -O  --orbsize  B : uses an orbit buffer size of B kB (default: 2048)\n");
  printf("   -S,  --sync     : try to emit orbits at the LHC rate \n");
  printf("   -n, --nclients N : runs N clients for timelices 0..N-1 with increasing port numbers\n");
  printf("\n");
  return retval;
}

void start_and_run(std::unique_ptr<GeneratorBase> &&generator,
                   const std::string &target,
                   unsigned int norbits,
                   unsigned int firstOrbit,
                   unsigned int orbitmux,
                   int iclient,
                   std::atomic<unsigned int> *nclients,
                   std::atomic<unsigned int> *client_errors) {
  printf("Starting generator %d to %s\n", iclient, target.c_str());
  int fd;
  auto pos = target.find(':');
  std::string filename;
  if (pos == std::string::npos) {
    pos = target.find("%d");
    if (pos == std::string::npos) {
      filename = target;
    } else {
      filename = target.substr(0, pos) + std::to_string(iclient) + target.substr(pos + 2);
    }
    fd = open(filename.c_str(), O_WRONLY | O_TRUNC | O_CREAT);
  } else {
    std::string ip = target.substr(0, pos), port = target.substr(pos + 1);
    fd = connect_tcp(ip.c_str(), port.c_str(), iclient);
  }
  if (fd < 0) {
    printf("Error in opening target %s for client %d.\n", target.c_str(), iclient);
    (*client_errors)++;
    return;
  }
  (*nclients)--;
  while ((*nclients) > 0 && (*client_errors) == 0)
    ;  // ugly spinlock
  if ((*client_errors) != 0) {
    printf("Skipping generator %d, since there were errors in the setup\n", iclient);
  } else {
    auto tstart = std::chrono::steady_clock::now();
    generator->generate(fd, norbits, firstOrbit, orbitmux);
    auto tend = std::chrono::steady_clock::now();
    auto dt = (std::chrono::duration<double>(tend - tstart)).count();
    double orbrate = norbits / orbitmux / dt, orbrate_lhc = 40e6 / 3564;
    printf("%02u: Generator done, wrote %u orbits in %.2f ms (%.3f kHz, x %.3f)\n",
           iclient,
           norbits / orbitmux,
           dt * 1000,
           orbrate / 1000,
           orbrate / orbrate_lhc);
    close(fd);
    if (!filename.empty())
      chmod(filename.c_str(), S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  }
}

int main(int argc, char **argv) {
  int tmux = 6, tmux_slice = 0, orbsize_kb = 2048, nclients = 1, seed = 37;
  unsigned int orbits = 0, orbitmux = 1, seconds = 0, run = 37, lumisection = 1, orbitsPerLumi = 1 << 10;
  bool sync = false;
  while (1) {
    static struct option long_options[] = {{"help", no_argument, nullptr, 'h'},
                                           {"tmux", required_argument, nullptr, 'T'},
                                           {"orbitmux", required_argument, nullptr, 1},
                                           {"orbits", required_argument, nullptr, 2},
                                           {"time", required_argument, nullptr, 3},
                                           {"orbitsPerLumi", required_argument, nullptr, 4},
                                           {"tslice", required_argument, nullptr, 't'},
                                           {"seed", required_argument, nullptr, 's'},
                                           {"run", required_argument, nullptr, 'r'},
                                           {"ls", required_argument, nullptr, 'l'},
                                           {"orbsize", required_argument, nullptr, 'O'},
                                           {"nclients", required_argument, nullptr, 'n'},
                                           {"sync", no_argument, nullptr, 'S'},
                                           {nullptr, 0, nullptr, 0}};
    /* getopt_long stores the option index here. */
    int option_index = 0;
    int optc = getopt_long(argc, argv, "hd:T:t:O:n:Sr:l:s:", long_options, &option_index);

    /* Detect the end of the options. */
    if (optc == -1)
      break;

    switch (optc) {
      case 'h':
        return print_usage(argv[0], 0);
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
        orbits = std::atol(optarg);
        break;
      case 3: {
        std::string optval(optarg);
        char unit = optval[optval.length() - 1];
        if (unit == 's')
          seconds = std::atol(optval.substr(0, optval.length() - 1).c_str());
        else if (unit == 'm')
          seconds = std::atol(optval.substr(0, optval.length() - 1).c_str()) * 60;
        else if (unit == 'h')
          seconds = std::atol(optval.substr(0, optval.length() - 1).c_str()) * 3600;
        else {
          printf("Unsupported argument for --time: %s\n", optarg);
          return 1;
        }
      } break;
      case 4:
        orbitsPerLumi = std::atol(optarg);
        break;
      case 'O':
        orbsize_kb = std::atoi(optarg);
        break;
      case 's':
        seed = std::atoi(optarg);
        break;
      case 'l':
        lumisection = std::atoi(optarg);
        break;
      case 'r':
        run = std::atoi(optarg);
        break;
      case 'S':
        sync = true;
        break;
      default:
        return print_usage(argv[0], 1);
    }
  }

  int nargs = argc - optind;
  if (nargs < 3)
    return print_usage(argv[0], 1);

  std::string kind = argv[optind++];
  std::string srcFile = argv[optind++];
  ResamplerData srcData(srcFile);
  if (srcData.events.empty()) {
    printf("File %s can't be read, or contains no events\n", srcFile.c_str());
    return 3;
  }

  std::string target = argv[optind++];

  if (seconds != 0) {
    if (orbits != 0) {
      printf("Can't specify both --orbits and --time\n");
      return 1;
    }
    orbits = (40e6 / 3564) * seconds;
  } else if (orbits == 0) {
    orbits = 10;
  }
  unsigned int firstOrbit = 1;
  if (lumisection > 1) {
    firstOrbit += orbitsPerLumi * (lumisection - 1);
  }
  int ret = 0;
  std::atomic<unsigned int> clients = nclients, client_errors = 0;
  std::vector<std::thread> client_threads;
  for (int client = 0; client < nclients; ++client) {
    std::unique_ptr<GeneratorBase> checker;
    if (kind == "Native64") {
      checker.reset(new Native64Generator(srcData, tmux, tmux_slice + client, seed + 37 * client, orbsize_kb * 1024));
    } else if (kind == "DTHBasic256") {
      checker.reset(
          new DTH256Generator(srcData, tmux, tmux_slice + client, seed + 37 * client, sync, orbsize_kb * 1024));
    } else if (kind == "CMSSW") {
      checker.reset(new CMSSWGenerator(
          srcData, tmux, tmux_slice + client, seed + 37 * client, run, lumisection, orbsize_kb * 1024));
    } else {
      printf("Unsupported mode '%s'\n", kind.c_str());
      return 3;
    }
    client_threads.emplace_back(
        start_and_run, std::move(checker), target, orbits, firstOrbit, orbitmux, client, &clients, &client_errors);
  }
  for (auto &t : client_threads)
    t.join();
  if (client_errors.load() > 0) {
    ret = 1;
  }
  return ret;
}