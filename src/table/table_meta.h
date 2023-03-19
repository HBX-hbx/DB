#ifndef DBTRAIN_TABLE_META_H
#define DBTRAIN_TABLE_META_H

#include "../defines.h"
#include "../record/field.h"
#include "../record/record.h"
#include "../result/result.h"

namespace dbtrain {

struct Column {
  FieldType type_;
  size_t len_;
  std::string name_;
};

class TableMeta {
 public:
  TableMeta() = default;
  ~TableMeta() = default;

  int Load(const uint8_t *src);
  int Store(uint8_t *dst);
  size_t GetSize() const;
  size_t GetLength() const;
  PageID GetTableEnd() const;

 private:
  vector<Column> cols_;

  // TIPS: Meta 信息，可自行添加字段

  // int record_length_; // 每条记录的长度
  // int record_per_page_; // 每一页存放多少条记录
  int table_end_page_; // 表的最后一页 / 表的总页面数
  PageID first_free_; // 第一个有空闲槽位的页面
  // int bitmap_length_; // 页头 bitmap 的长度

  friend class Table;
  friend class PageHandle;
  friend class RecordFactory;
};

}  // namespace dbtrain

#endif
