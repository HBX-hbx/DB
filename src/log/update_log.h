#ifndef DBTRAIN_UPDATE_LOG_H
#define DBTRAIN_UPDATE_LOG_H

#include "basic_log.h"
#include "log_image.h"
#include "log_manager.h"

namespace dbtrain {

class UpdateLog : public TxLog {
 public:
  UpdateLog() = default;
  UpdateLog(LSN lsn, LSN prev_lsn, XID xid);
  ~UpdateLog() = default;

  void Load(const Byte *src) override;
  size_t Store(Byte *dst) override;

  void Redo();
  void Undo();
  UniquePageID GetUniPageID() const;

  LogType GetType() const override;
  size_t GetLength() const override;

 private:
  PhysiologicalImage log_image_;

  friend class LogFactory;
  friend class CLRLog;
};

class CLRLog : public UpdateLog {
 public:
  CLRLog() = default;
  CLRLog(LSN lsn, LSN prev_lsn, XID xid, LSN undo_next_lsn);
  ~CLRLog() = default;

  void Load(const Byte *src) override;
  size_t Store(Byte *dst) override;

  LogType GetType() const override;
  size_t GetLength() const override;
  LSN GetUndoNextLSN();
 private:
  LSN undo_next_lsn_;
};

}  // namespace dbtrain

#endif
