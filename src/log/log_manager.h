#ifndef DBTRAIN_LOG_MANAGER_H
#define DBTRAIN_LOG_MANAGER_H

#include <map>
#include <mutex>

#include "../defines.h"
#include "log.h"

namespace dbtrain {

struct UniquePageID {
  string table_name;
  PageID page_id;
  bool operator<(const UniquePageID &uid) const {
    if (table_name != uid.table_name) {
      return table_name < uid.table_name;
    } else {
      return page_id < uid.page_id;
    }
  }
};

class LogManager {
 public:
  static LogManager &GetInstance();
  ~LogManager() = default;
  LogManager(const LogManager &) = delete;
  void operator=(const LogManager &) = delete;

  void Begin(XID xid);
  void Commit(XID xid);
  void Abort(XID xid);
  void Checkpoint();

  void InsertRecordLog(XID xid, const string &table_name, Rid rid, size_t new_len, const void *new_val);
  void DeleteRecordLog(XID xid, const string &table_name, Rid rid, size_t old_len, const void *old_val);
  void UpdateRecordLog(XID xid, const string &table_name, Rid rid, size_t old_len, const void *old_val, size_t new_len,
                       const void *new_val);

  void WritePage(int fd, PageID page_id);
  // 切换数据库初始化
  void Close();
  LSN GetCurrent() const;
  void Init();

  void Analyse(LSN checkpoint_lsn);
  void Redo();
  void Undo();
  bool Undo(XID xid);

 private:
  LogManager();
  void WriteLog(Log *log);
  LSN AppendLog();

  std::map<XID, LSN> att_;
  std::map<UniquePageID, LSN> dpt_;

  // TIPS: 仅需在设计Log缓存时使用
  // TIPS: 所有更新时间>FlushedLSN的页面不能被写回
  // TIPS: 需要定期将日志写入磁盘来更新FlushedLSN
  LSN flushed_lsn_;
  LSN current_lsn_;
  LSN checkpoint_lsn_;

  std::mutex log_mutex_;
  std::mutex lsn_mutex_;

  friend class CheckpointLog;
};

}  // namespace dbtrain

#endif
