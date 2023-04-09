#include "update_log.h"

#include "../system/system_manager.h"
#include "../record/record_factory.h"
#include "defines.h"
#include "log/log.h"
#include "log/log_image.h"
#include "log/log_manager.h"

namespace dbtrain {

typedef PhysiologicalImage LogImage;

UpdateLog::UpdateLog(LSN lsn, LSN prev_lsn, XID xid) : TxLog(lsn, prev_lsn, xid) {}

void UpdateLog::Redo() {
  // TODO: 根据Image进行Redo操作
  // TIPS: 需要先结合PageLSN判断是否需要进行Redo (上层保证)
  // TIPS: 可以直接基于Image定位页面并按照操作类型进行Redo
  // TIPS: 基础功能不需要考虑Meta信息和页头信息的变化
  // LAB 2 BEGIN
  Table* table = SystemManager::GetInstance().GetTable(log_image_.table_name_);
  PageHandle page_handle = table->GetPage(log_image_.page_id_);
  TableMeta meta = table->GetMeta();
  RecordFactory record_factory(&meta);

  switch (log_image_.op_type_) {
    case PhysiologicalImage::LogOpType::INSERT : {
      page_handle.InsertRecord(log_image_.slot_id_, log_image_.new_val_, lsn_);
      break;
    }
    case PhysiologicalImage::LogOpType::DELETE : {
      page_handle.DeleteRecord(log_image_.slot_id_, lsn_);
      break;
    }
    case PhysiologicalImage::LogOpType::UPDATE : {
      page_handle.UpdateRecord(log_image_.slot_id_, log_image_.new_val_, lsn_);
      break;
    }
    default: {
      assert(false);
    }
  }
  // LAB 2 END
}

void UpdateLog::Undo() {
  // TODO: 根据Image进行Undo操作
  // TIPS: 可以直接基于Image定位页面并按照操作类型进行Undo
  // TIPS: 基础功能不需要考虑Meta信息和页头信息的变化
  // LAB 2 BEGIN
  Table* table = SystemManager::GetInstance().GetTable(log_image_.table_name_);
  PageHandle page_handle = table->GetPage(log_image_.page_id_);
  TableMeta meta = table->GetMeta();
  RecordFactory record_factory(&meta);

  switch (log_image_.op_type_) {
    case PhysiologicalImage::LogOpType::INSERT : {
      page_handle.DeleteRecord(log_image_.slot_id_, lsn_);
      break;
    }
    case PhysiologicalImage::LogOpType::DELETE : {
      page_handle.InsertRecord(log_image_.slot_id_, log_image_.old_val_, lsn_);
      break;
    }
    case PhysiologicalImage::LogOpType::UPDATE : {
      page_handle.UpdateRecord(log_image_.slot_id_, log_image_.old_val_, lsn_);
      break;
    }
    default: {
      assert(false);
    }
  }
  PhysiologicalImage clr_log_image;
  clr_log_image.table_name_ = log_image_.table_name_;
  clr_log_image.page_id_    = log_image_.page_id_;
  clr_log_image.slot_id_    = log_image_.slot_id_;
  clr_log_image.new_len_    = log_image_.new_len_;
  clr_log_image.old_len_    = log_image_.old_len_;
  clr_log_image.op_type_    = log_image_.op_type_;
  clr_log_image.new_val_ = new Byte[clr_log_image.new_len_];
  clr_log_image.old_val_ = new Byte[clr_log_image.old_len_];
  memcpy(clr_log_image.new_val_, log_image_.new_val_, log_image_.new_len_);
  memcpy(clr_log_image.old_val_, log_image_.old_val_, log_image_.old_len_);
  // 添加 CLR 日志，即 Undo 的 Redo 日志
  
  LogManager::GetInstance().CLR(
    xid_, 
    prev_lsn_, 
    log_image_
  );
  // LAB 2 END
}

void UpdateLog::Load(const Byte *src) {
  TxLog::Load(src);
  auto src_ = src + 2 * sizeof(LSN) + sizeof(XID);
  log_image_.Load(src_);
}

size_t UpdateLog::Store(Byte *dst) {
  size_t length = TxLog::Store(dst);
  auto dst_ = dst + length;
  size_t img_length = log_image_.Store(dst_);
  return length + img_length;
}

UniquePageID UpdateLog::GetUniPageID() const { return {log_image_.table_name_, log_image_.page_id_}; }

LogType UpdateLog::GetType() const { return LogType::UPDATE; }

size_t UpdateLog::GetLength() const { return TxLog::GetLength() + log_image_.GetLength(); }

void CLRLog::Load(const Byte *src) {
  TxLog::Load(src);
  auto src_ = src + 2 * sizeof(LSN) + sizeof(XID);
  memcpy(&undo_next_lsn_, src_, sizeof(LSN));
  src_ = src_ + sizeof(LSN);
  log_image_.Load(src_);
}

size_t CLRLog::Store(Byte *dst) {
  size_t length = TxLog::Store(dst);
  memcpy(dst + length, &undo_next_lsn_, sizeof(LSN));
  auto dst_ = dst + length + sizeof(LSN);
  size_t img_length = log_image_.Store(dst_);
  return length + sizeof(LSN) + img_length;
}

LogType CLRLog::GetType() const { return LogType::CLR; }

size_t CLRLog::GetLength() const { return TxLog::GetLength() + sizeof(LSN) + log_image_.GetLength(); }

LSN CLRLog::GetUndoNextLSN() { return undo_next_lsn_; }

CLRLog::CLRLog(LSN lsn, LSN prev_lsn, XID xid, LSN undo_next_lsn) : UpdateLog(lsn, prev_lsn, xid), undo_next_lsn_(undo_next_lsn) {}

}  // namespace dbtrain
