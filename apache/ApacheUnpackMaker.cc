#include "ApacheUnpackMaker.h"

#include "IPCUnpackerBase.h"
#include "IPCUnpackerFloats.h"
#include "IPCUnpackerInts.h"
#include "IPCUnpackerRaw64.h"
#include "IPCUnpackerTkMuFloats.h"

ApacheUnpackMaker::Spec::Spec(const std::string &obj,
                              const std::string &kind,
                              const std::string &unpackFormat,
                              const std::string &compression,
                              int level)
    : format(unpackFormat), compressionAlgo(compression), compressionLevel(level) {
  if (obj == "puppi") {
    objType = ObjType::Puppi;
  } else if (obj == "tkmu") {
    objType = ObjType::Puppi;
  } else {
    throw std::invalid_argument("Unsupported object type '" + obj + "', must be 'puppi' or 'tkmu'");
  }
  if (kind == "ipc") {
    fileKind = FileKind::IPC;
  } else {
    throw std::invalid_argument("Unsupported file kind '" + kind + "', must be 'ipc'");
  }
}
std::unique_ptr<UnpackerBase> ApacheUnpackMaker::make(const ApacheUnpackMaker::Spec &spec, unsigned int batchsize) {
  std::unique_ptr<UnpackerBase> unpacker;
  switch (spec.objType) {
    case Spec::ObjType::Puppi:
      switch (spec.fileKind) {
        case Spec::FileKind::IPC:
          if (spec.format == "float" || spec.format == "float16") {
            unpacker = std::make_unique<IPCUnpackerFloats>(batchsize, spec.format == "float16");
          } else if (spec.format == "int") {
            unpacker = std::make_unique<IPCUnpackerInts>(batchsize);
          } else if (spec.format == "raw64") {
            unpacker = std::make_unique<IPCUnpackerRaw64>(batchsize);
          } else {
            throw std::invalid_argument("Unsupported puppi ipc format " + spec.format);
          }
      }
      break;
    case Spec::ObjType::TkMu:
      switch (spec.fileKind) {
        case Spec::FileKind::IPC:
          if (spec.format == "float" || spec.format == "float16") {
            unpacker = std::make_unique<IPCUnpackerTkMuFloats>(batchsize, spec.format == "float16");
          } else {
            throw std::invalid_argument("Unsupported tkmu ipc format " + spec.format);
          }
      }
      break;
  }
  if (unpacker)
    unpacker->setCompression(spec.compressionAlgo, spec.compressionLevel);
  return unpacker;
}