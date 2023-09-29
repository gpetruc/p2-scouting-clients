#ifndef p2_clients_RootUnpackMaker_h
#define p2_clients_RootUnpackMaker_h
#include "../UnpackerBase.h"
#include <memory>

namespace RootUnpackMaker {
  struct Spec {
    enum class ObjType { Puppi, TkMu };
    enum class FileKind { TTree, RNTuple };
    ObjType objType;
    FileKind fileKind;
    std::string format;
    std::string compressionAlgo;
    int compressionLevel;
    Spec(const std::string &obj,   // puppi or tkmu
         const std::string &kind,  // ttree or rntuple
         const std::string &unpackFormat,
         const std::string &compression = "none",
         int level = 0);
    Spec(const ObjType &obj,    // puppi or tkmu
         const FileKind &kind,  // ttree or rntuple
         const std::string &unpackFormat,
         const std::string &compression = "none",
         int level = 0)
        : objType(obj), fileKind(kind), format(unpackFormat), compressionAlgo(compression), compressionLevel(level) {}
  };
  std::unique_ptr<UnpackerBase> make(const Spec &spec);
  inline std::unique_ptr<UnpackerBase> make(const std::string &object,  // puppi or tkmu
                                            const std::string &kind,    // ttree or rntuple
                                            const std::string &format,
                                            const std::string &compression = "none",
                                            int level = 0) {
    return make(RootUnpackMaker::Spec(object, kind, format, compression, level));
  }
};  // namespace RootUnpackMaker

#endif