#include "checkpoint_log.h"

#include <cassert>

#include "../tx/tx_manager.h"
#include "defines.h"

namespace dbtrain {

CheckpointLog::CheckpointLog() : Log() {}
CheckpointLog::CheckpointLog(LSN lsn) : Log(lsn) {}
void CheckpointLog::Load(const Byte *src) {
  Log::Load(src);
  size_t fsize = sizeof(LSN);
  LogManager &log_manager = LogManager::GetInstance();
  // TODO: 恢复当前事务编号
  // LAB 3 BEGIN
  XID xid;
  memcpy(&xid, src + fsize, sizeof(XID));
  fsize += sizeof(XID);
  TxManager::GetInstance().SetXID(xid);
  // LAB 3 END
  // TODO: 加载MasterRecord对应的Checkpoint Log
  // TIPS: 利用读取的信息更新LogManager
  // LAB 2 BEGIN
  // map size
  size_t len_of_att = 0;
  memcpy(&len_of_att, src + fsize, sizeof(size_t));
  fsize += sizeof(size_t);
  log_manager.att_.clear();
  // map content
  for (size_t i = 0; i < len_of_att; ++i) {
    XID xid;
    LSN lsn;
    memcpy(&xid, src + fsize, sizeof(XID));
    fsize += sizeof(XID);
    memcpy(&lsn, src + fsize, sizeof(LSN));
    fsize += sizeof(LSN);

    log_manager.att_[xid] = lsn;
  }
  // map size
  size_t len_of_dpt = 0;
  memcpy(&len_of_dpt, src + fsize, sizeof(size_t));
  fsize += sizeof(size_t);
  log_manager.dpt_.clear();
  // map content
  for (size_t i = 0; i < len_of_dpt; ++i) {
    UniquePageID upid;
    LSN lsn;
    // variable length string
    size_t len_of_string = 0;
    memcpy(&len_of_string, src + fsize, sizeof(size_t));
    fsize += sizeof(size_t);
    char* name = new char[len_of_string];
    memcpy(name, src + fsize, len_of_string);
    fsize += len_of_string;
    upid.table_name.assign(name);

    memcpy(&upid.page_id, src + fsize, sizeof(PageID));
    fsize += sizeof(PageID);
    memcpy(&lsn, src + fsize, sizeof(LSN));
    fsize += sizeof(LSN);

    log_manager.dpt_[upid] = lsn;
  }
  // LAB 2 END
}

size_t CheckpointLog::Store(Byte *dst) {
  LogManager &log_manager = LogManager::GetInstance();
  size_t fsize = Log::Store(dst);
  // TODO: 存储当前事务编号
  // LAB 3 BEGIN
  XID xid = TxManager::GetInstance().GetXID();
  memcpy(dst + fsize, &xid, sizeof(XID));
  fsize += sizeof(XID);
  // LAB 3 END
  // TODO: 存储LogManager相关信息，返回Store的数据长度
  // TIPS: 不添加缓存机制情况下，仅需要保存ATT和DPT
  // TIPS: 考虑缓存机制情况下，需要额外存储Flushed LSN
  // LAB 2 BEGIN
  // map size
  size_t len_of_att = log_manager.att_.size();
  memcpy(dst + fsize, &len_of_att, sizeof(size_t));
  fsize += sizeof(size_t);
  // map content
  for (auto pair: log_manager.att_) {
    XID xid = pair.first;
    LSN lsn = pair.second;
    memcpy(dst + fsize, &xid, sizeof(XID));
    fsize += sizeof(XID);
    memcpy(dst + fsize, &lsn, sizeof(LSN));
    fsize += sizeof(LSN);
  }
  // map size
  size_t len_of_dpt = log_manager.dpt_.size();
  memcpy(dst + fsize, &len_of_dpt, sizeof(size_t));
  fsize += sizeof(size_t);
  // map content
  for (auto pair: log_manager.dpt_) {
    UniquePageID upid = pair.first;
    LSN lsn = pair.second;
    // variable length string
    size_t len_of_string = upid.table_name.size() + 1;
    memcpy(dst + fsize, &len_of_string, sizeof(size_t));
    fsize += sizeof(size_t);
    memcpy(dst + fsize, upid.table_name.c_str(), len_of_string);
    fsize += len_of_string;

    memcpy(dst + fsize, &upid.page_id, sizeof(PageID));
    fsize += sizeof(PageID);
    memcpy(dst + fsize, &lsn, sizeof(LSN));
    fsize += sizeof(LSN);
  }
  // LAB 2 END
  return fsize;
}
LogType CheckpointLog::GetType() const { return LogType::CHECKPOINT; }

size_t CheckpointLog::GetLength() const {
  size_t length = Log::GetLength() + 2 * sizeof(LSN) + sizeof(XID);
  length += 2 * sizeof(size_t);
  LogManager &log_manager = LogManager::GetInstance();
  length += log_manager.att_.size() * (sizeof(XID) + sizeof(LSN));
  length += log_manager.dpt_.size() * (sizeof(LSN) + sizeof(PageID) + sizeof(size_t));
  for (const auto &dpt_pair : log_manager.dpt_) {
    length += dpt_pair.first.table_name.size();
  }
  return length;
}

}  // namespace dbtrain
