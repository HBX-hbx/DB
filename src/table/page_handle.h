#ifndef DBTRAIN_PAGE_HANDLE_H
#define DBTRAIN_PAGE_HANDLE_H

#include <set>

#include "../defines.h"
#include "../storage/buffer_manager.h"
#include "../storage/page.h"
#include "../table/table_meta.h"
#include "../utils/bitmap.h"

namespace dbtrain {

// TIPS: 可自行添加字段
struct PageHeader {
  LSN page_lsn;
  PageID next_free;
  unsigned int record_num;
};

struct RecordInfo {
  short length;  // 记录的长度
  short offset;  // 记录偏移量
  bool  is_del;  // 是否被删除
};

class PageHandle {
  friend class Table;

 public:
  PageHandle() = default;
  PageHandle(Page *page, const TableMeta &meta);
  ~PageHandle() = default;

  // 无并发接口
  void InsertRecord(Record *record, bool* success);
  void DeleteRecord(SlotID slot_no);
  void UpdateRecord(SlotID slot_no, Record *record, bool* success);
  RecordList LoadRecords();

  void Display();

  // LAB 2: 新增部分函数方便后续实验
  uint8_t *GetRaw(SlotID slot_no);
  Record *GetRecord(SlotID slot_no);

  void InsertRecord(SlotID slot_no, const void *data, LSN lsn);
  void DeleteRecord(SlotID slot_no, LSN lsn);
  void UpdateRecord(SlotID slot_no, const void *data, LSN lsn);

  // LAB 3: MVCC接口
  void InsertRecord(Record *record, XID xid);
  // 第三个参数没有实际含义，仅用于区分重载函数 DeleteRecord(SlotID slot_no, LSN lsn)
  void DeleteRecord(SlotID, XID xid, bool);
  RecordList LoadRecords(XID xid, const std::set<XID> &uncommit_xids);

  Rid Next();

  bool Full();
  PageID GetNextFree();
  SlotID FirstFree();
  void SetLSN(LSN lsn);
  LSN GetLSN();

 private:
  // Bitmap bitmap_;
  PageHeader *header_;
  // int record_length_;
  RecordInfo* record_info_; // 记录的长度和偏移量(相对 slots)
  int* record_offset_; // 剩余空间结束位置(相对 slots)
  uint8_t *slots_;  // 指向页尾
  Page *page_;
  TableMeta meta_;
};

}  // namespace dbtrain

#endif
