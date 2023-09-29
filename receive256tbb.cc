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
#include <sys/wait.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <thread>
#include <atomic>
#include <vector>

#include <getopt.h>
#include <tbb/pipeline.h>
#include <tbb/global_control.h>

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

  void init(unsigned int tmux, unsigned int firstBx = 0, unsigned int orbitMux = 1) {
    tmux_ = tmux;
    firstbx_ = firstBx;
    orbitMux_ = orbitMux;
    id_ = firstbx_;
    clear();
  }
  void clear() {
    orbits_ = 0;
    truncorbits_ = 0;
    bytes_ = 0;
    wroteBytes_ = 0;
  }
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
    explicit DTH_Header(bool isok) : ok(isok) {}
    uint8_t size128;
    bool ok, start, end;
  };
  struct DTH_Header256 {
    explicit DTH_Header256(bool isok) : ok(isok) {}
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
    DTH_Header ret(ok);
    if (!ret.ok)
      return ret;
    ret.start = test_bit<47>(out);
    ret.end = test_bit<46>(out);
    ret.size128 = out[48 / 8];
    assert(out[0] == 0x47 && out[1] == 0x5a);
    assert(ret.end || (ret.size128 == 0xFF));
    if (checkStart)
      assert(ret.start);
    if (checkEnd)
      assert(ret.end);
    return ret;
  }
  DTH_Header256 readDTH256(int sockfd, uint8_t out[32], bool checkStart = false, bool checkEnd = false) {
    int n = read(sockfd, reinterpret_cast<char *>(out), 32);
    assert(n <= 0 || n == 32);
    bool ok = (n == 32);
    return parseDTH256(ok, out, checkStart, checkEnd);
  }
  DTH_Header256 parseDTH256(bool ok, const uint8_t out[32], bool checkStart = false, bool checkEnd = false) {
    DTH_Header256 ret(ok);
    if (!ret.ok)
      return ret;
    ret.start = test_bit<47>(out);
    ret.end = test_bit<46>(out);
    ret.size256 = out[48 / 8];
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

protected:
  unsigned int id_, tmux_, firstbx_, orbitMux_;
  std::atomic<uint32_t> orbits_, truncorbits_;
  std::atomic<uint64_t> bytes_, wroteBytes_;
};

class DTHRollingReceive256 : public CheckerBase {
public:
  DTHRollingReceive256(unsigned int orbSize_kb,
                       const char *basepath,
                       unsigned int nbuffers,
                       unsigned int ntasks,
                       unsigned int orbitsPerFile,
                       unsigned int prescale = 1)
      : CheckerBase(),
        orbSize_(orbSize_kb * 1024),
        orbitsPerFile_(orbitsPerFile),
        prescale_(prescale),
        basepath_(basepath),
        fname_(),
        fout_(),
        nbuffers_(nbuffers),
        ntasks_(ntasks) {
    assert(nbuffers >= ntasks);
    for (unsigned int i = 0; i < nbuffers_; ++i) {
      buffers_.emplace_back(orbSize_kb);
    }
  }

  ~DTHRollingReceive256() override {
    for (auto & b : buffers_) b.wipe();
  }

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
  void newFile(uint32_t orbitNo) {
    if (fout_.is_open()) {
      assert(fname_.length() > 4);
      rename(fname_.c_str(), fname_.substr(0, fname_.length() - 4).c_str());
    }
    fout_.close();
    char buff[1024];
    snprintf(buff, 1023, "%s.ts%02d.orb%08u.dump.tmp", basepath_.c_str(), firstbx_, orbitNo);
    fname_ = buff;
    fout_.open(buff, std::ios_base::binary | std::ios_base::out | std::ios_base::trunc);
  }

  struct Buffer {
    unsigned int capacity;
    uint8_t *start, *end;
    mutable unsigned int size, orbit;
    Buffer() : capacity(0), start(nullptr), end(nullptr), size(0), orbit(0) {}
    Buffer(unsigned int sizeInKb)
        : capacity(sizeInKb * 1024),
          start(reinterpret_cast<uint8_t *>(std::aligned_alloc(4096u, capacity))),
          end(start + capacity),
          size(0),
          orbit(0) {}
    void wipe() {
      if (capacity)
        std::free(start);
      capacity = 0;
      start = nullptr;
      end = nullptr;
      size = 0;
      orbit = 0;
    }
  };

  Buffer *acquire(int in) {
    uint8_t buff256[32];
    Buffer *b = &buffers_[ibuffer_];
    if (++ibuffer_ == nbuffers_)
      ibuffer_ = 0;
    uint8_t *ptr = b->start, *end = b->end;
    DTH_Header256 dthh = readDTH256(in, buff256, true, false);
    if (++orbits_ == 1) {
      tstart_ = std::chrono::steady_clock::now();
    }
    uint32_t totlen256 = dthh.size256;
    assert(ptr + (dthh.size256 << 5) < end);
    dthh.ok = readChunk(in, ptr, dthh.size256, !dthh.end);
    while (dthh.ok && !dthh.end) {
      dthh = parseDTH256(dthh.ok, ptr, false, false);
      if (!dthh.ok)
        break;
      totlen256 += dthh.size256;
      assert(ptr + (dthh.size256 << 5) < end);
      dthh.ok = readChunk(in, ptr, dthh.size256, !dthh.end);
    }
    if (!dthh.ok) {
      orbits_--; // I had increased it before
      return nullptr;
    }
    ptr = b->start;
    if (totlen256 <= 1) {
      truncorbits_++;
    } else {
      bytes_ += totlen256 << 5;
    }
    uint64_t header = *reinterpret_cast<const uint64_t *>(ptr);
    b->orbit = (header >> 24) & 0xFFFFFFFF;
    b->size = totlen256 << 5;
    return b;
  }

  void consume(Buffer *b) {
    unsigned int orbit = b->orbit;
    if (!fout_.is_open() || orbit % orbitsPerFile_ == orbitMux_)
      newFile(orbit);
    if (prescale_ != 0 && (prescale_ == 1 || (orbit % prescale_ == 1))) {
      wroteBytes_ += b->size;
      fout_.write(reinterpret_cast<char *>(b->start), b->size);
    }
    if (--toprint_ == 0) {
      printf("%02u: Read %7u orbits (%7u truncated, %.4f%%), %9.3f GB, wrote %9.3f GB.\n",
             id_,
             orbits_.load(),
             truncorbits_.load(),
             truncorbits_.load() * 100.0 / orbits_.load(),
             bytes_.load() / (1024. * 1024. * 1024.),
             wroteBytes_.load() / (1024. * 1024. * 1024.));
      toprint_ = nprint_;
    }
    b->orbit = 0;
    b->size = 0;
  }

  int run(int in) override {
    nprint_ = 1000;
    toprint_ = nprint_;
    ibuffer_ = 0;
    auto head =
        tbb::make_filter<void, Buffer *>(tbb::filter::serial_in_order, [in, this](tbb::flow_control &fc) -> Buffer * {
          Buffer *b = acquire(in);
          if (!b)
            fc.stop();
          return b;
        });
    auto tail =
        tbb::make_filter<Buffer *, void>(tbb::filter::serial_in_order, [in, this](Buffer *b) -> void { consume(b); });

    printf("Set up running with %u buffers, %u tasks\n", nbuffers_, ntasks_);
    tbb::parallel_pipeline(ntasks_, head & tail);

    if (fout_.is_open()) {
      assert(fname_.length() > 4);
      rename(fname_.c_str(), fname_.substr(0, fname_.length() - 4).c_str());
      fout_.close();
    }

    auto tend = std::chrono::steady_clock::now();
    std::chrono::duration<double> dt = tend - tstart_;
    double readRate = bytes_.load() / 1024.0 / 1024.0 / 1024.0 / dt.count();
    double wroteRate = wroteBytes_.load() / 1024.0 / 1024.0 / 1024.0 / dt.count();
    printf("%02u: Read %u orbits (%u truncated, %.4f%%), %.3f GB.\n",
           id_,
           orbits_.load(),
           truncorbits_.load(),
           truncorbits_.load() * 100.0 / orbits_.load(),
           bytes_.load() / (1024. * 1024. * 1024.));
    printf("%02u: Wrote %.3f GB, %lu b\n", id_, wroteBytes_.load() / (1024. * 1024. * 1024.),  wroteBytes_.load());
    printf("%02u: Data rate in %.3f GB/s, out %.3f GB/s\n", id_, readRate, wroteRate);
    printf("\n");
    return 0;
  }

protected:
  unsigned int orbSize_;
  unsigned int orbitsPerFile_, prescale_;
  std::string basepath_, fname_;
  std::fstream fout_;
  std::vector<Buffer> buffers_;
  unsigned int nbuffers_, ntasks_, nprint_;
  mutable unsigned int ibuffer_, toprint_;
  std::chrono::time_point<std::chrono::steady_clock> tstart_;
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

int print_usage(const char *self, int retval) {
  printf("Usage: %s  [options] Algo ( file | ip:port ) arguments options \n", self);
  printf("Algo: DTHRollingReceive256\n");
  printf("   -T, --tmux  T  : runs at TMUX T (default: 6)\n");
  printf("    --orbitmux N  : mux orbits by factor N (default: 1)\n");
  printf("   -t, --tslice T : runs tslice t  (default: 0)\n");
  printf("   -B, --buffsize B : uses a read buffer size of B kB  (default: 4)\n");
  printf("   -O  --orbsize  B : uses an orbit buffer size of B kB (default: 2048)\n");
  printf("   -k  --keep    : keep running \n");
  printf("   -f  --fork    : use fork instead of threads.\n");
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
    if (ret)
      errors->fetch_add(1);
    checker->clear();
  } while (keep_running);
  printf("Done in client %d\n", client);
}

void start_and_run_and_exit(std::unique_ptr<CheckerBase> &&checker, const std::string &src, int client) {
  printf("Starting in client %d\n", client);
  int sourcefd = open_source(src, client);
  if (sourcefd < 0) {
    printf("Error in opening source %s for client %d.\n", src.c_str(), client);
    _exit(1);
  }
  int ret = checker->run(sourcefd);
  printf("Done in client %d, ret = %d\n", client, ret);
  _exit(ret ? 2 : 0);
}

int main(int argc, char **argv) {
  int tmux = 6, tmux_slice = 0, orbitmux = 1, nbuffers = 1, ntasks = 1, orbsize_kb = 2048, nclients = 1, prescale = 1;
  bool keep_running = false, usefork = false;;
  while (1) {
    static struct option long_options[] = {{"help", no_argument, nullptr, 'h'},
                                           {"keep", no_argument, nullptr, 'k'},
                                           {"debug", required_argument, nullptr, 'd'},
                                           {"tmux", required_argument, nullptr, 'T'},
                                           {"orbitmux", required_argument, nullptr, 1},
                                           {"nbuffers", required_argument, nullptr, 'b'},
                                           {"ntasks", required_argument, nullptr, 'j'},
                                           {"tslice", required_argument, nullptr, 't'},
                                           {"orbsize", required_argument, nullptr, 'O'},
                                           {"nclients", required_argument, nullptr, 'n'},
                                           {"prescale", required_argument, nullptr, 'p'},
                                           {"fork", no_argument, nullptr, 'f'},
                                           {nullptr, 0, nullptr, 0}};
    /* getopt_long stores the option index here. */
    int option_index = 0;
    int optc = getopt_long(argc, argv, "khd:T:t:B:O:n:p:b:fij:", long_options, &option_index);

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
      case 'b':
        nbuffers = std::atoi(optarg);
        break;
      case 'j':
        ntasks = std::atoi(optarg);
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
      case 'f':
        usefork = true;
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
  std::vector<pid_t> child_pids;
  tbb::global_control tbb_ctrl(tbb::global_control::max_allowed_parallelism, ntasks*(usefork ? 1 : nclients));
  for (int client = 0; client < nclients; ++client) {
    std::unique_ptr<CheckerBase> checker;
    if (kind == "DTHRollingReceive256") {
      if (nargs != 4) {
        printf("Usage: %s DTHRollingReceive256 ip:port outfile orbitsPerFile \n", argv[0]);
        return 3;
      }
      checker.reset(
          new DTHRollingReceive256(orbsize_kb, argv[optind], nbuffers, ntasks, std::atoi(argv[optind + 1]), prescale));
    } else {
      printf("Unsupported mode '%s'\n", kind.c_str());
      return 3;
    }
    if (orbitmux == 1) {
      checker->init(tmux, tmux_slice + client, orbitmux);
    } else {
      checker->init(tmux, tmux_slice, orbitmux);
      checker->setId(client);
    }
    if (usefork) {
      pid_t childpid = fork();
      if (childpid == 0) {
        // I'm the child
        start_and_run_and_exit(std::move(checker), src, client);
      } else {
        child_pids.push_back(childpid);
      }
    } else {
      client_threads.emplace_back(start_and_run, std::move(checker), src, client, keep_running, &client_errors);
    }
  }
  if (usefork) {
    for (int client = 0; client < nclients; ++client) {
      int status;
      pid_t childpid = wait(&status);
      if (status == 1) {
        for (pid_t other : child_pids) {
          if (other != 0 && other != childpid) {
            kill(other, 9);
          }
        }
      }
      if (status != 0)
        ret = 1;
      for (int i = 0; i < nclients; ++i)
        if (child_pids[i] == childpid)
          child_pids[i] = 0;
    }
  } else {
    for (auto &t : client_threads)
      t.join();
    if (client_errors > 0)
      ret = 1;
  }
  return ret;
}