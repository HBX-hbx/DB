#ifndef DBTRAIN_BASIC_LOG_H
#define DBTRAIN_BASIC_LOG_H

#include "log.h"

namespace dbtrain {

class TxLog : public Log {
 public:
  TxLog() = default;
  ~TxLog() = default;
  TxLog(LSN lsn, LSN prev_lsn, XID xid);
  void Load(const Byte *src) override;
  size_t Store(Byte *dst) override;
  size_t GetLength() const override;

  LSN GetPrevLSN() const;
  XID GetXID() const;

 protected:
  LSN prev_lsn_;
  XID xid_;
};

class BeginLog : public TxLog {
 public:
  BeginLog() = default;
  BeginLog(LSN lsn, LSN prev_lsn, XID xid);
  LogType GetType() const override;
};

class EndLog : public TxLog {
 public:
  EndLog() = default;
  EndLog(LSN lsn, LSN prev_lsn, XID xid);
  LogType GetType() const override;
};

class CommitLog : public TxLog {
 public:
  CommitLog() = default;
  CommitLog(LSN lsn, LSN prev_lsn, XID xid);
  LogType GetType() const override;
};

class AbortLog : public TxLog {
 public:
  AbortLog() = default;
  AbortLog(LSN lsn, LSN prev_lsn, XID xid);
  LogType GetType() const override;
};

class CrashHereLog : public TxLog {
 public:
  CrashHereLog() = default;
  CrashHereLog(LSN lsn, LSN prev_lsn, XID xid);
  LogType GetType() const override;
};

}  // namespace dbtrain

#endif