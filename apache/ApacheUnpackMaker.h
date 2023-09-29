#ifndef p2_clients_apache_ApacheUnpackMaker_h
#define p2_clients_apache_ApacheUnpackMaker_h
#include "../UnpackerBase.h"
#include <memory>

namespace ApacheUnpackMaker {
  struct Spec {
    enum class ObjType { Puppi, TkMu };
    enum class FileKind { IPC };
    ObjType objType;
    FileKind fileKind;
    std::string format;
    std::string compressionAlgo;
    int compressionLevel;
    Spec(const std::string &obj,   // puppi
         const std::string &kind,  // ipc
         const std::string &unpackFormat,
         const std::string &compression = "none",
         int level = 0);
    Spec(const ObjType &obj,
         const FileKind &kind,
         const std::string &unpackFormat,
         const std::string &compression = "none",
         int level = 0)
        : objType(obj), fileKind(kind), format(unpackFormat), compressionAlgo(compression), compressionLevel(level) {}
  };
  std::unique_ptr<UnpackerBase> make(const Spec &spec, unsigned int batchsize);
  inline std::unique_ptr<UnpackerBase> make(unsigned int batchsize,
                                            const std::string &object,  // puppi
                                            const std::string &kind,    // ipc
                                            const std::string &format,
                                            const std::string &compression = "none",
                                            int level = 0) {
    return make(ApacheUnpackMaker::Spec(object, kind, format, compression, level), batchsize);
  }
}  // namespace ApacheUnpackMaker

#endif