#ifndef DBTRAIN_LOG_H
#define DBTRAIN_LOG_H

#include "../defines.h"

namespace dbtrain {

const LSN NULL_LSN = UINT32_MAX;

enum class LogType { UNDEFINED = 0, UPDATE, COMMIT, ABORT, BEGIN, END, CLR, CHECKPOINT };

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