#include "log_factory.h"

#include "logs.h"
#include "record/record.h"

namespace dbtrain {

Log *LogFactory::LoadLog(const Byte *src) {
  Log *log = nullptr;
  LogType log_type = LogType::UNDEFINED;
  memcpy(&log_type, src, sizeof(LogType));
  if (log_type == LogType::BEGIN) {
    log = new BeginLog();
  } else if (log_type == LogType::ABORT) {
    log = new AbortLog();
  } else if (log_type == LogType::COMMIT) {
    log = new CommitLog();
  } else if (log_type == LogType::CHECKPOINT) {
    log = new CheckpointLog();
  } else if (log_type == LogType::UPDATE) {
    log = new UpdateLog();
  } else {
    assert(false);
  }
  log->Load(src + sizeof(LogType));
  return log;
}

size_t LogFactory::StoreLog(Byte *dst, Log *log) {
  // TIPS: 建议将类型存在起始位置
  auto log_type = log->GetType();
  memcpy(dst, &log_type, sizeof(LogType));
  size_t length = log->Store(dst + sizeof(LogType)) + sizeof(LogType);
  return length;
}

Log *LogFactory::NewDeleteLog(const TxInfo &info, const string &table_name, Rid rid, size_t old_len,
                              const void *old_val) {
  UpdateLog *log = new UpdateLog(info.lsn, info.prev_lsn, info.xid);
  log->log_image_.page_id_ = rid.page_no;
  log->log_image_.slot_id_ = rid.slot_no;
  log->log_image_.table_name_ = table_name;

  log->log_image_.op_type_ = PhysiologicalImage::LogOpType::DELETE;
  log->log_image_.old_len_ = old_len;
  log->log_image_.old_val_ = new Byte[old_len];
  memcpy(log->log_image_.old_val_, old_val, old_len);
  return log;
}

Log *LogFactory::NewInsertLog(const TxInfo &info, const string &table_name, Rid rid, size_t new_len,
                              const void *new_val) {
  UpdateLog *log = new UpdateLog(info.lsn, info.prev_lsn, info.xid);
  log->log_image_.page_id_ = rid.page_no;
  log->log_image_.slot_id_ = rid.slot_no;
  log->log_image_.table_name_ = table_name;

  log->log_image_.op_type_ = PhysiologicalImage::LogOpType::INSERT;
  log->log_image_.new_len_ = new_len;
  log->log_image_.new_val_ = new Byte[new_len];
  memcpy(log->log_image_.new_val_, new_val, new_len);
  return log;
}

Log *LogFactory::NewUpdateLog(const TxInfo &info, const string &table_name, Rid rid, size_t old_len,
                              const void *old_val, size_t new_len, const void *new_val) {
  UpdateLog *log = new UpdateLog(info.lsn, info.prev_lsn, info.xid);
  log->log_image_.page_id_ = rid.page_no;
  log->log_image_.slot_id_ = rid.slot_no;
  log->log_image_.table_name_ = table_name;

  log->log_image_.op_type_ = PhysiologicalImage::LogOpType::UPDATE;
  log->log_image_.old_len_ = old_len;
  log->log_image_.old_val_ = new Byte[old_len];
  memcpy(log->log_image_.old_val_, old_val, old_len);
  log->log_image_.new_len_ = new_len;
  log->log_image_.new_val_ = new Byte[new_len];
  memcpy(log->log_image_.new_val_, new_val, new_len);
  return log;
}

}  // namespace dbtrain
