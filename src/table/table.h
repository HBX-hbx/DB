#ifndef DBTRAIN_TABLE_H
#define DBTRAIN_TABLE_H

#include <string>

#include "../defines.h"
#include "../record/record.h"
#include "../result/result.h"
#include "../storage/buffer_manager.h"
#include "../table/page_handle.h"
#include "../table/table_meta.h"

namespace dbtrain {

class Table {
 public:
  Table(const std::string &table_name, int meta_fd, int data_fd);
  Table(const std::string &table_name, int meta_fd, int data_fd, const std::vector<Column> &columns);
  ~Table();

  Result Desc();

  TableMeta GetMeta() const;
  void SetMeta(const TableMeta meta);

  int Load(const uint8_t *src);
  int Store(uint8_t *dst);

  void InsertRecord(Record *record);
  void DeleteRecord(const Rid &rid);
  void UpdateRecord(const Rid &rid, Record *record);
  Record *GetRecord(const Rid &rid);

  Record *operator[](int idx);
  int GetColumnIdx(string col_name) const;
  int GetColumnSize() const;

  void GC();
  PageID GetFirstFree();

 public:
  PageHandle CreatePage();
  PageHandle GetPage(PageID page_id);
  string GetName() const;
  vector<string> GetColumnNames() const;
  FieldType GetColumnType(int col_idx) const;
  void StoreMeta();

 private:
  TableMeta meta_;

  std::string table_name_;
  int data_fd_;
  int meta_fd_;

  bool meta_modified = false;

  BufferManager &buffer_manager_;

  bool IsHiddenColumn(const string &col_name) const;
};

}  // namespace dbtrain

#endif
