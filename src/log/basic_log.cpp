#include "basic_log.h"

namespace dbtrain {

TxLog::TxLog(LSN lsn, LSN prev_lsn, XID xid) : Log(lsn), prev_lsn_(prev_lsn), xid_(xid) {}
void TxLog::Load(const Byte *src) {
  Log::Load(src);
  memcpy(&prev_lsn_, src + sizeof(LSN), sizeof(LSN));
  memcpy(&xid_, src + 2 * sizeof(prev_lsn_), sizeof(XID));
}
size_t TxLog::Store(Byte *dst) {
  size_t length = Log::Store(dst);
  memcpy(dst + length, &prev_lsn_, sizeof(LSN));
  memcpy(dst + length + sizeof(LSN), &xid_, sizeof(XID));
  return length + sizeof(LSN) + sizeof(XID);
}
LSN TxLog::GetPrevLSN() const { return prev_lsn_; }
XID TxLog::GetXID() const { return xid_; }
size_t TxLog::GetLength() const { return Log::GetLength() + sizeof(LSN) + sizeof(XID); }

BeginLog::BeginLog(LSN lsn, LSN prev_lsn, XID xid) : TxLog(lsn, prev_lsn, xid) {}
EndLog::EndLog(LSN lsn, LSN prev_lsn, XID xid) : TxLog(lsn, prev_lsn, xid) {}
CommitLog::CommitLog(LSN lsn, LSN prev_lsn, XID xid) : TxLog(lsn, prev_lsn, xid) {}
AbortLog::AbortLog(LSN lsn, LSN prev_lsn, XID xid) : TxLog(lsn, prev_lsn, xid) {}
CrashHereLog::CrashHereLog(LSN lsn, LSN prev_lsn, XID xid) : TxLog(lsn, prev_lsn, xid) {}

LogType BeginLog::GetType() const { return LogType::BEGIN; }
LogType EndLog::GetType() const { return LogType::END; }
LogType CommitLog::GetType() const { return LogType::COMMIT; }
LogType AbortLog::GetType() const { return LogType::ABORT; }
LogType CrashHereLog::GetType() const { return LogType::UNDO_CRASH_HERE; }

}  // namespace dbtrain
