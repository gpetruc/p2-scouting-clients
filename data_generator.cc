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

#include <iostream>
#include <random>
#include <algorithm>
#include <getopt.h>

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
    for (unsigned int bx = offs_; bx < NBX; bx += tmux_) {
      const auto &item = data_->events[rnd_() % nevents];
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
    virtual void generate(int fd, unsigned int norbits, unsigned int orbitmux) = 0;

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
  void generate(int fd, unsigned int norbits, unsigned int orbitmux) override final {
    uint64_t *orbit_buff = reinterpret_cast<uint64_t *>(std::aligned_alloc(4096u, orbSize_));
    uint64_t *end_buff = orbit_buff + (orbSize_ / sizeof(uint64_t));
    for (unsigned int i = 1; i <= norbits; i += orbitmux) {
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
                    unsigned int orbitSize = 2 * 1024 * 1024)
      : GeneratorBase(data, tmux, offs, seed), orbSize_(orbitSize) {}
  void generate(int fd, unsigned int norbits, unsigned int orbitmux) override final {
    uint64_t *orbit_buff = reinterpret_cast<uint64_t *>(std::aligned_alloc(4096u, orbSize_));
    uint64_t *end_buff = orbit_buff + (orbSize_ / sizeof(uint64_t));
    uint8_t header[32]; // 256 bits
    std::fill(header, header+32, 0);
    header[0] = 0x47;
    header[1] = 0x5a;
    for (unsigned int i = 1; i <= norbits; i += orbitmux) {
      unsigned int nwords = src_.fillOrbit(i, orbit_buff, end_buff);
      bool first = true;
      uint64_t *ptr = orbit_buff;
      // add 3 null words for safety
      for (int w = 0; w < 3; ++w) orbit_buff[nwords+w] = 0;
      for (int n256 = (nwords + 3) >> 2; n256 > 0; n256 -= 255) {
        unsigned int chunksize256 = std::min<int>(n256, 255);
        header[6] = chunksize256;
        header[5] = (first ? (1<<7) : 0) | (n256 <= 255 ? (1<<6) : 0);
        write(fd, header, 32);
        write(fd, ptr, (chunksize256 << 2) * sizeof(uint64_t));
        ptr += (chunksize256 << 2);
        first = false;
      }
    }
    std::free(orbit_buff);
  }

private:
  unsigned int orbSize_;
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
  printf("   -T, --tmux  T  : runs at TMUX T (default: 6)\n");
  printf("    --orbitmux N  : mux orbits by factor N (default: 1)\n");
  printf("    --maxorbits N : stop after this number of orbits (default: 10000000)\n");
  printf("   -t, --tslice T : runs tslice t  (default: 0)\n");
  printf("   -O  --orbsize  B : uses an orbit buffer size of B kB (default: 2048)\n");
  printf("   -k  --keep    : keep running \n");
  printf("   -n, --nclients N : runs N clients for timelices 0..N-1 with increasing port numbers\n");
  printf("\n");
  return retval;
}

void start_and_run(std::unique_ptr<GeneratorBase> &&generator,
                   const std::string &target,
                   unsigned int norbits,
                   unsigned int orbitmux,
                   int iclient,
                   std::atomic<unsigned int> *nclients,
                   std::atomic<unsigned int> *client_errors) {
  printf("Starting generator %d to %s\n", iclient, target.c_str());
  int fd;
  auto pos = target.find(':');
  if (pos == std::string::npos) {
    pos = target.find("%d");
    if (pos == std::string::npos) {
      fd = open(target.c_str(), O_WRONLY | O_TRUNC | O_CREAT);
    } else {
      std::string fname = target.substr(0, pos) + std::to_string(iclient) + target.substr(pos + 2);
      fd = open(fname.c_str(), O_WRONLY | O_TRUNC | O_CREAT);
    }
  } else {
    std::string ip = target.substr(0, pos), port = target.substr(pos + 1);
    fd = connect_tcp(ip.c_str(), port.c_str(), iclient);
  }
  if (fd < 0) {
    printf("Error in opening target %s for client %d.\n", target.c_str(), iclient);
    client_errors++;
    return;
  }
  (*nclients)--;
  while ((*nclients) > 0 && (*client_errors) == 0)
    ;  // ugly spinlock
  if ((*client_errors) != 0) {
    printf("Skipping generator %d, since there were errors in the setup\n", iclient);
  } else {
    auto tstart = std::chrono::steady_clock::now();
    generator->generate(fd, norbits, orbitmux);
    auto tend = std::chrono::steady_clock::now();
    auto dt = (std::chrono::duration<double>(tend - tstart)).count();
    double orbrate = norbits/orbitmux/dt, orbrate_lhc = 40e6 / 3564;
    printf("%02u: Done, wrote %u orbits in %.2f ms (%.3f kHz, x %.3f)\n",
           iclient,
           norbits/orbitmux,
           dt *  1000,
           orbrate / 1000,
           orbrate/orbrate_lhc);
  }
}

int main(int argc, char **argv) {
  int tmux = 6, tmux_slice = 0, orbsize_kb = 2048, nclients = 1, seed = 37;
  unsigned int maxorbits = 10, orbitmux = 1;
  while (1) {
    static struct option long_options[] = {{"help", no_argument, nullptr, 'h'},
                                           {"tmux", required_argument, nullptr, 'T'},
                                           {"orbitmux", required_argument, nullptr, 1},
                                           {"orbits", required_argument, nullptr, 2},
                                           {"tslice", required_argument, nullptr, 't'},
                                           {"seed", required_argument, nullptr, 's'},
                                           {"orbsize", required_argument, nullptr, 'O'},
                                           {"nclients", required_argument, nullptr, 'n'},
                                           {nullptr, 0, nullptr, 0}};
    /* getopt_long stores the option index here. */
    int option_index = 0;
    int optc = getopt_long(argc, argv, "hd:T:t:O:n:z", long_options, &option_index);

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
        maxorbits = std::atol(optarg);
        break;
      case 'O':
        orbsize_kb = std::atoi(optarg);
        break;
      case 's':
        seed = std::atoi(optarg);
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

  std::string target = argv[optind++];

  int ret = 0;
  std::atomic<unsigned int> clients = nclients, client_errors = 0;
  std::vector<std::thread> client_threads;
  for (int client = 0; client < nclients; ++client) {
    std::unique_ptr<GeneratorBase> checker;
    if (kind == "Native64") {
      checker.reset(new Native64Generator(srcData, tmux, tmux_slice + client, seed + 37 * client, orbsize_kb * 1024));
    } else if (kind == "DTHBasic256") {
      checker.reset(new DTH256Generator(srcData, tmux, tmux_slice + client, seed + 37 * client, orbsize_kb * 1024));
    } else {
      printf("Unsupported mode '%s'\n", kind.c_str());
      return 3;
    }
    client_threads.emplace_back(start_and_run, std::move(checker), target, maxorbits, orbitmux, client, &clients, &client_errors);
  }
  for (auto &t : client_threads)
    t.join();
  if (client_errors > 0)
    ret = 1;
  return ret;
}