#include "table_meta.h"
#include <iostream>

#include "../record/fields.h"

namespace dbtrain {

int TableMeta::Load(const uint8_t *src) {
  // TODO: 元信息反序列化
  // TIPS: 函数目标是通过src反序列化成Meta结构体，通过memcpy函数可以完成这一工作
  // TIPS: 需要注意将所有信息全部保存，变长信息可以通过“长度+数据”的方式存储
  // LAB 1 BEGIN
  std::cerr << "< ------------- TableMeta::Load --------------- >\n";
  size_t offset = 0;
  size_t size_of_cols = 0;
  memcpy(&size_of_cols, src + offset, sizeof(size_t));
  offset += sizeof(size_t);
  // std::cerr << "size_of_cols: " << size_of_cols << "\n";
  cols_.clear();
  Column col;
  for (size_t i = 0; i < size_of_cols; ++i) {
    memcpy(&col.type_, src + offset, sizeof(FieldType));
    offset += sizeof(FieldType);
    memcpy(&col.len_, src + offset, sizeof(size_t));
    offset += sizeof(size_t);
    // variable length string
    size_t len_of_string = 0;
    memcpy(&len_of_string, src + offset, sizeof(size_t));
    offset += sizeof(size_t);
    char* name = new char[len_of_string];
    memcpy(name, src + offset, len_of_string);
    offset += len_of_string;
    col.name_.assign(name);

    cols_.push_back(col);
  }
  // memcpy(&record_length_, src + offset, sizeof(int));
  // std::cerr << "record_length_: " << record_length_ << "\n";
  // offset += sizeof(int);
  // memcpy(&record_per_page_, src + offset, sizeof(int));
  // std::cerr << "record_per_page_: " << record_per_page_ << "\n";
  // offset += sizeof(int);
  memcpy(&table_end_page_, src + offset, sizeof(int));
  // std::cerr << "table_end_page_: " << table_end_page_ << "\n";
  offset += sizeof(int);
  memcpy(&first_free_, src + offset, sizeof(PageID));
  // std::cerr << "first_free_: " << first_free_ << "\n";
  offset += sizeof(PageID);
  // memcpy(&bitmap_length_, src + offset, sizeof(int));
  // std::cerr << "bitmap_length_: " << bitmap_length_ << "\n";
  // std::cerr << "cols_[0]: " << cols_[0].len_ << " " << int(cols_[0].type_) << "\n";

  return 0;
  // LAB 1 END
}

int TableMeta::Store(uint8_t *dst) {
  // TODO: 元信息序列化
  // TIPS: 函数目标是将Meta信息序列化到dst中，通过memcpy函数可以完成这一工作
  // TIPS: 需要注意将所有信息全部读取，变长信息需要结合Load函数解析
  // LAB 1 BEGIN
  std::cerr << "< ------------- TableMeta::Store --------------- >\n";
  size_t offset = 0;
  size_t size_of_cols = cols_.size();
  // variable length cols
  memcpy(dst + offset, &size_of_cols, sizeof(size_t));
  offset += sizeof(size_t);
  // std::cerr << "size_of_cols: " << size_of_cols << "\n";
  for (auto &col: cols_) {
    memcpy(dst + offset, &col.type_, sizeof(FieldType));
    offset += sizeof(FieldType);
    memcpy(dst + offset, &col.len_, sizeof(size_t));
    offset += sizeof(size_t);
    // variable length string
    size_t len_of_string = col.name_.size() + 1;
    memcpy(dst + offset, &len_of_string, sizeof(size_t));
    offset += sizeof(size_t);
    memcpy(dst + offset, col.name_.c_str(), len_of_string);
    offset += len_of_string;
  }
  // memcpy(dst + offset, &record_length_, sizeof(int));
  // std::cerr << "record_length_: " << record_length_ << "\n";
  // offset += sizeof(int);
  // memcpy(dst + offset, &record_per_page_, sizeof(int));
  // std::cerr << "record_per_page_: " << record_per_page_ << "\n";
  // offset += sizeof(int);
  memcpy(dst + offset, &table_end_page_, sizeof(int));
  // std::cerr << "table_end_page_: " << table_end_page_ << "\n";
  offset += sizeof(int);
  memcpy(dst + offset, &first_free_, sizeof(PageID));
  // std::cerr << "first_free_: " << first_free_ << "\n";
  offset += sizeof(PageID);
  // memcpy(dst + offset, &bitmap_length_, sizeof(int));
  // std::cerr << "bitmap_length_: " << bitmap_length_ << "\n";
  // std::cerr << "cols_[0]: " << cols_[0].len_ << " " << int(cols_[0].type_) << "\n";
  
  return 0;
  // LAB 1 END
}

size_t TableMeta::GetSize() const { return cols_.size(); }

size_t TableMeta::GetLength() const {
  size_t length = 0;
  for (const auto &col : cols_) {
    length += col.len_;
  }
  return length;
}

PageID TableMeta::GetTableEnd() const { return table_end_page_; }

}  // namespace dbtrain
