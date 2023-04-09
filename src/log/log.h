#ifndef DBTRAIN_LOG_H
#define DBTRAIN_LOG_H

#include "../defines.h"

namespace dbtrain {

const LSN NULL_LSN = UINT32_MAX;

enum class LogType { 
  UNDEFINED        = 0, 
  UPDATE           = 1, 
  COMMIT           = 2, 
  ABORT            = 3, 
  BEGIN            = 4, 
  END              = 5, 
  CLR              = 6, 
  CHECKPOINT       = 7, 
  BEGIN_CHECKPOINT = 8, 
  UNDO_CRASH_HERE  = 9, 
};

class Log {
 public:
  Log() = default;
  Log(LSN lsn);
  virtual ~Log() = default;

  virtual void Load(const Byte *src);
  virtual size_t Store(Byte *dst);

  virtual LogType GetType() const = 0;
  virtual size_t GetLength() const;
  LSN GetLSN() const;

 protected:
  LSN lsn_;
};

}  // namespace dbtrain

#endif