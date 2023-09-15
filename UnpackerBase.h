#ifndef p2_clients_unpacker_base_h
#define p2_clients_unpacker_base_h
#include <cstdio>
#include <cstdint>
#include <string>
#include <vector>

class UnpackerBase {
    public:
        UnpackerBase() {} 
        virtual ~UnpackerBase() {}
        virtual unsigned long int unpack(const std::vector<std::string> &ins, const std::string &out) const = 0;
        virtual void setThreads(unsigned int threads) = 0;
        virtual void setCompression(const std::string &algo, unsigned int level) = 0;
};

#endif