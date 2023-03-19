#include "page_handle.h"

#include <cassert>
#include <set>
#include <iostream>

#include "../record/record_factory.h"
#include "../tx/lock_manager.h"

namespace dbtrain {

PageHandle::PageHandle(Page *page, const TableMeta &meta)
    : page_(page),
      // record_length_(meta.record_length_),
      // bitmap_(page->GetData() + sizeof(PageHeader), meta.record_per_page_),
      meta_(meta) {
  // Display();
  slots_ = page->GetData() + PAGE_SIZE;
  header_ = (PageHeader *)page->GetData();
  record_offset_ = (int *)(page->GetData() + sizeof(PageHeader));
  record_info_ = (RecordInfo *)(page->GetData() + sizeof(int) + sizeof(PageHeader));
}

void PageHandle::InsertRecord(Record *record, bool* success) {
  std::cerr << "< ----------------- PageHandle::InsertRecord ---------------- >\n";
  // 获取排它锁
  LockManager &lock_manager = LockManager::GetInstance();
  lock_manager.Lock("Page" + std::to_string(page_->GetPageId().page_no));
  // TODO: 插入记录
  // LAB 1 BEGIN
  // TIPS: 获取第一个空槽
  int free_slot = -1;
  int record_info_size = header_->record_num;
  std::cerr << "size: " << record_info_size << "\n";
  std::cerr << " < ---- record display: --- >\n";
  // record->Display();
  // 获得已删除的槽位，查看是否放得下
  for (int i = 0; i < record_info_size; ++i) {
    if (record_info_[i].is_del && (record_info_[i].length >= record->GetLength())) {
      free_slot = i;
      break;
    }
  }
  std::cerr << "before free_slot: " << free_slot << "\n";
  std::cerr << "before record offset: " << *record_offset_ << "\n";
  if (free_slot == -1) {
    // 没有空位或者放不下
    int remain_space = PAGE_SIZE - *record_offset_ - sizeof(PageHeader) - sizeof(int) - sizeof(RecordInfo) * header_->record_num;
    if (remain_space < record->GetLength()) {
      // 该页插入失败
      std::cerr << "space overflow!\n";
      *success = false;
      // 释放排它锁
      lock_manager.Unlock("Page" + std::to_string(page_->GetPageId().page_no));
      return;
    }
    free_slot = record_info_size; // 在尾部插入
    record_info_[free_slot] = RecordInfo();
    header_->record_num++;
    std::cerr << "record_num++: from " << header_->record_num - 1 << " to " << header_->record_num << "\n"; 
    // 更新 剩余空间结束位置 record_offset_
    *record_offset_ += record->GetLength();
    record_info_[free_slot].offset = *record_offset_;
  } else {

  }
  record_info_[free_slot].is_del = false;
  record_info_[free_slot].length = record->GetLength();
  std::cerr << "after free_slot: " << free_slot << "\n";
  std::cerr << "after record offset: " << *record_offset_ << "\n";
  // TIPS: 使用RecordFactory::SetRid设置record的rid
  Rid rid;
  rid.page_no = page_->GetPageId().page_no;
  rid.slot_no = free_slot;
  RecordFactory::SetRid(record, rid);
  // TIPS: 使用RecordFactory的StoreRecord方法将record序列化到页面中
  RecordFactory record_factory(&meta_);
  record_factory.StoreRecord(slots_ - record_info_[free_slot].offset, record);
  // TIPS: 将page_标记为dirty
  page_->SetDirty();
  *success = true;
  // LAB 1 END
  // LAB 2: 设置页面LSN
  SetLSN(LogManager::GetInstance().GetCurrent());
  // 释放排它锁
  lock_manager.Unlock("Page" + std::to_string(page_->GetPageId().page_no));
}

void PageHandle::DeleteRecord(SlotID slot_no) {
  std::cerr << "< ----------------- PageHandle::DeleteRecord ---------------- >\n";
  // 获取排他锁
  LockManager &lock_manager = LockManager::GetInstance();
  lock_manager.Lock("Page" + std::to_string(page_->GetPageId().page_no));
  // TODO: 删除记录
  // TIPS: 直接设置bitmap_为0即可删除对应记录
  // TIPS: 将page_标记为dirty
  // LAB 1 BEGIN
  assert(slot_no < header_->record_num);
  std::cerr << "deleting slot: " << slot_no << "\n";
  record_info_[slot_no].is_del = true;
  page_->SetDirty();
  // LAB 1 END
  // LAB 2: 设置页面LSN
  SetLSN(LogManager::GetInstance().GetCurrent());
  // 释放排他锁
  lock_manager.Unlock("Page" + std::to_string(page_->GetPageId().page_no));
}

void PageHandle::UpdateRecord(SlotID slot_no, Record *record, bool *success) {
  std::cerr << "< ----------------- PageHandle::UpdateRecord ---------------- >\n";
  // 获取排他锁
  LockManager &lock_manager = LockManager::GetInstance();
  lock_manager.Lock("Page" + std::to_string(page_->GetPageId().page_no));
  // TODO: 更新记录
  // TIPS: 将page_标记为dirty
  // LAB 1 BEGIN
  assert(slot_no < header_->record_num);
  RecordFactory record_factory(&meta_);
  if (record_info_[slot_no].length >= record->GetLength()) {
    // 原位更新
    record_info_[slot_no].length = record->GetLength();
    record_info_[slot_no].is_del = false;
    record_factory.StoreRecord(slots_ - record_info_[slot_no].offset, record);
  } else {
    // 删除原位置
    std::cerr << "deleting slot: " << slot_no << "\n";
    record_info_[slot_no].is_del = true;
    int remain_space = PAGE_SIZE - *record_offset_ - sizeof(PageHeader) - sizeof(int) - sizeof(RecordInfo) * header_->record_num;
    if (remain_space < record->GetLength()) {
      // 该页插入失败
      std::cerr << "space overflow!\n";
      *success = false;
      // 释放排它锁
      lock_manager.Unlock("Page" + std::to_string(page_->GetPageId().page_no));
      return;
    }
    // 更新 剩余空间结束位置 record_offset_
    *record_offset_ += record->GetLength();
    RecordInfo new_record_info;
    new_record_info.is_del = false;
    new_record_info.length = record->GetLength();
    new_record_info.offset = *record_offset_;
    // 在尾部插入新的
    Rid rid;
    rid.page_no = page_->GetPageId().page_no;
    rid.slot_no = header_->record_num;
    record_factory.SetRid(record, rid);

    record_info_[header_->record_num] = new_record_info;
    header_->record_num++;
    std::cerr << "record_num++: from " << header_->record_num - 1 << " to " << header_->record_num << "\n"; 
    record_factory.StoreRecord(slots_ - *record_offset_, record);
  }
  *success = true;
  page_->SetDirty();
  // LAB 1 END
  // 设置页面LSN
  SetLSN(LogManager::GetInstance().GetCurrent());
  // 释放排他锁
  lock_manager.Unlock("Page" + std::to_string(page_->GetPageId().page_no));
}

void PageHandle::Display() {
  std::cerr << "< ------- Display -------- >\n";
  for (size_t i = 0; i < PAGE_SIZE; i++) {
    printf("%02X ", page_->GetData()[i]);
  }
  std::cerr << "\n< ------- Display End -------- >\n";
}

RecordList PageHandle::LoadRecords() {
  std::cerr << "< ----------------- PageHandle::LoadRecords:1 ---------------- >\n";
  // 获取共享锁
  LockManager &lock_manager = LockManager::GetInstance();
  lock_manager.LockShared("Page" + std::to_string(page_->GetPageId().page_no));
  int slot_no = -1;
  RecordFactory record_factory(&meta_);
  std::vector<Record *> record_vector;

  int record_info_size = header_->record_num;
  std::cerr << "record_info_size: " << header_->record_num << "\n";
  for (int i = 0; i < record_info_size; ++i) {
    if (!record_info_[i].is_del) {
      std::cerr << "< ----------------- finding one slot not free ---------------- >\n";
      std::cerr << "slot: " << i << "\n";
      Record *record = record_factory.LoadRecord(slots_ - record_info_[i].offset);
      record_vector.push_back(record);
    }
  }
  // 释放共享锁
  lock_manager.UnlockShared("Page" + std::to_string(page_->GetPageId().page_no));
  return record_vector;
}

uint8_t *PageHandle::GetRaw(SlotID slot_no) { return slots_ - record_info_[slot_no].offset; }

Record *PageHandle::GetRecord(SlotID slot_no) {
  // 获取共享锁
  LockManager &lock_manager = LockManager::GetInstance();
  lock_manager.LockShared("Page" + std::to_string(page_->GetPageId().page_no));

  if (!record_info_[slot_no].is_del) {
    RecordFactory record_factory(&meta_);
    Record *record = record_factory.LoadRecord(slots_ - record_info_[slot_no].offset);
    // 释放共享锁
    lock_manager.UnlockShared("Page" + std::to_string(page_->GetPageId().page_no));
    return record;
  } else {
    // 释放共享锁
    lock_manager.UnlockShared("Page" + std::to_string(page_->GetPageId().page_no));
    return nullptr;
  }
}

void PageHandle::InsertRecord(SlotID slot_no, const void *src, LSN lsn) {
  // 获取排他锁
  LockManager &lock_manager = LockManager::GetInstance();
  lock_manager.Lock("Page" + std::to_string(page_->GetPageId().page_no));

  record_info_[slot_no].is_del = false;
  memcpy(slots_ - record_info_[slot_no].offset, src, record_info_[slot_no].length);
  page_->SetDirty();
  // 设置页面LSN
  SetLSN(lsn);

  // 释放排他锁
  lock_manager.Unlock("Page" + std::to_string(page_->GetPageId().page_no));
}

void PageHandle::DeleteRecord(SlotID slot_no, LSN lsn) {
  // 获取排他锁
  LockManager &lock_manager = LockManager::GetInstance();
  lock_manager.Lock("Page" + std::to_string(page_->GetPageId().page_no));

  record_info_[slot_no].is_del = true;
  page_->SetDirty();
  // 设置页面LSN
  SetLSN(lsn);

  // 释放排他锁
  lock_manager.Unlock("Page" + std::to_string(page_->GetPageId().page_no));
}

void PageHandle::UpdateRecord(SlotID slot_no, const void *src, LSN lsn) {
  // 获取排他锁
  LockManager &lock_manager = LockManager::GetInstance();
  lock_manager.Lock("Page" + std::to_string(page_->GetPageId().page_no));

  assert(!record_info_[slot_no].is_del);
  memcpy(slots_ - record_info_[slot_no].offset, src, record_info_[slot_no].length);
  page_->SetDirty();
  // 设置页面LSN
  SetLSN(lsn);

  // 释放排他锁
  lock_manager.Unlock("Page" + std::to_string(page_->GetPageId().page_no));
}

void PageHandle::InsertRecord(Record *record, XID xid) {
  // TODO: MVCC情况下的数据插入
  // TIPS: 注意需要利用锁保证页面仅能同时被单个线程修改
  // TIPS: 注意MVCC需要设置版本号，版本号可以用事务号表示
  // LAB 3 BEGIN
  // LAB 3 END
}

void PageHandle::DeleteRecord(SlotID slot_no, XID xid, bool) {
  // TODO: MVCC情况下的数据删除
  // TIPS: 注意需要利用锁保证页面仅能同时被单个线程修改
  // TIPS: 注意MVCC删除不能直接清除数据，只是设置对应记录失效
  // LAB 3 BEGIN
  // LAB 3 END
}

RecordList PageHandle::LoadRecords(XID xid, const std::set<XID> &uncommit_xids) {
  std::cerr << "< ----------------- PageHandle::LoadRecords:2 ---------------- >\n";
  // 获取共享锁
  LockManager &lock_manager = LockManager::GetInstance();
  lock_manager.LockShared("Page" + std::to_string(page_->GetPageId().page_no));
  // 生成判定集合
  assert(uncommit_xids.find(xid) == uncommit_xids.end());

  int slot_no = -1;
  std::vector<Record *> record_vector;
  RecordFactory record_factory(&meta_);

  int record_info_size = header_->record_num;
  std::cerr << "record_info_size: " << header_->record_num << "\n";
  for (int i = 0; i < record_info_size; ++i) {
    if (!record_info_[i].is_del) {
      std::cerr << "< ----------------- finding one slot not free ---------------- >\n";
      std::cerr << "slot: " << i << "\n";
      Record *record = record_factory.LoadRecord(slots_ - record_info_[i].offset);
      record_vector.push_back(record);
    }
  }
  // 释放共享锁
  lock_manager.UnlockShared("Page" + std::to_string(page_->GetPageId().page_no));
  return record_vector;
}

Rid PageHandle::Next() { return Rid{0, 0}; }

// bool PageHandle::Full() { return bitmap_.Full(); }

PageID PageHandle::GetNextFree() { return header_->next_free; }

// SlotID PageHandle::FirstFree() { return bitmap_.FirstFree(); }

void PageHandle::SetLSN(LSN lsn) { header_->page_lsn = lsn; }

LSN PageHandle::GetLSN() { return header_->page_lsn; }

}  // namespace dbtrain
