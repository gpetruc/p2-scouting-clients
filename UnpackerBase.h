#ifndef p2_clients_unpacker_base_h
#define p2_clients_unpacker_base_h
#include <cstdio>
#include <cstdint>
#include <string>
#include <vector>

class UnpackerBase {
public:
  struct Report {
    unsigned long int entries;
    float bytes_in;
    float bytes_out;
    float time;
    explicit Report(unsigned long int e, float t = 0, float i = 0, float o = 0)
        : entries(e), bytes_in(i), bytes_out(o), time(t) {}
    Report &operator+=(const Report &other) {
      entries += other.entries;
      bytes_in += other.bytes_in;
      bytes_out += other.bytes_out;
      time += other.time;
      return *this;
    }
  };
  UnpackerBase() {}
  virtual ~UnpackerBase() {}
  virtual Report unpack(const std::vector<std::string> &ins, const std::string &out) const = 0;
  virtual void setThreads(unsigned int threads) = 0;
  virtual void setCompression(const std::string &algo, unsigned int level) = 0;
};

#endif