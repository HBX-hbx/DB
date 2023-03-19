#include "record_factory.h"
#include <cstddef>
#include <iostream>
#include "../exception/exceptions.h"
#include "../exception/record_exceptions.h"
#include "field.h"
#include "fields.h"
#include "../table/hidden.h"
#include "float_field.h"
#include "int_field.h"
#include "record.h"
#include "str_field.h"

namespace dbtrain {

RecordFactory::RecordFactory(const TableMeta *meta) : meta_(meta) {}

RecordFactory::~RecordFactory() {}

Record *RecordFactory::ConstInt(int v) {
  auto record = new Record();
  record->field_list_.push_back(new IntField(v));
  return record;
}

Record *RecordFactory::ConstFloat(double v) {
  auto record = new Record();
  record->field_list_.push_back(new FloatField(v));
  return record;
}

Record *RecordFactory::ConstString(string v) {
  auto record = new Record();
  record->field_list_.push_back(new StrField(v.c_str(), v.size()));
  return record;
}

Field *RecordFactory::LoadField(const void *src, FieldType ft, FieldSize fs) {
  std::cerr << "< ---------------- RecordFactory::LoadField ---------------- >\n";
  // TODO: 字段反序列化
  // TIPS: 利用Field的Load函数，分类返回对应类型的Field指针
  // LAB 1 BEGIN
  Field * field;
  if (ft == FieldType::INT) {
    field = new IntField();
  } else if (ft == FieldType::FLOAT) {
    field = new FloatField();
  } else if (ft == FieldType::STRING) {
    field = new StrField(fs);
  }
  field->Load(src, fs);
  return field;
  // LAB 1 END
}

void RecordFactory::StoreField(void *dst, Field *field, FieldType ft, FieldSize fs) {
  std::cerr << "< ---------------- RecordFactory::StoreField ---------------- >\n";
  // TODO: 字段序列化
  // TIPS: 利用Field的Store函数
  // LAB 1 BEGIN
  if (ft == FieldType::INT) {
    field = dynamic_cast<IntField *>(field);
  } else if (ft == FieldType::FLOAT) {
    field = dynamic_cast<FloatField *>(field);
  } else if (ft == FieldType::STRING) {
    field = dynamic_cast<StrField *>(field);
  }
  field->Store(dst, fs);
  // LAB 1 END
}

Record *RecordFactory::LoadRecord(const void *src) const {
  std::cerr << "< ---------------- RecordFactory::LoadRecord ---------------- >\n";
  // printf("src: %p\n", src);
  std::cerr << "src: " << (size_t)src << "\n";
  // TODO: 记录反序列化
  // TIPS: 通过TableMeta可以读取各个字段的属性和长度，利用LoadField函数建立各个字段对应的Field指针。
  // LAB 1 BEGIN
  Record* record = new Record();
  std::cerr << "meta col size: " << meta_->GetSize() << "\n";
  size_t offset = 0;
  vector<int> var_len_idx;
  for (int i = 0; i < meta_->GetSize(); ++i) {
    record->field_list_.push_back(new IntField());
    Column col = meta_->cols_[i];
    if (col.type_ == FieldType::STRING) {
      var_len_idx.push_back(i);
      continue;
    }
    Field* field = LoadField(static_cast<const uint8_t *>(src) + offset, col.type_, col.len_);
    offset += col.len_;
    record->SetField(i, field);
  }
  std::cerr << "before load var offset + length + data, offset: " << offset << "\n";
  for (size_t i = 0; i < var_len_idx.size(); ++i) {
    // 先取 <offset: int, length: int>
    std::cerr << "loading addr: " << (size_t)(static_cast<const uint8_t *>(src) + offset) << "\n";
    IntField *offset_field = dynamic_cast<IntField*>(LoadField(static_cast<const uint8_t *>(src) + offset, FieldType::INT, 4));
    offset += 4;
    IntField *length_field = dynamic_cast<IntField*>(LoadField(static_cast<const uint8_t *>(src) + offset, FieldType::INT, 4));
    offset += 4;
    // 取 var data
    auto col = meta_->cols_[var_len_idx[i]];
    std::cerr << "offset: " << offset_field->GetValue() << " length: " << length_field->GetValue() << "\n";
    Field *field = LoadField(static_cast<const uint8_t *>(src) + offset_field->GetValue(), col.type_, length_field->GetValue());
    record->SetField(var_len_idx[i], field);
  }
  // std:: cerr << " < ----- record display ----- >\n";
  // record->Display();
  return record;
  // LAB 1 END
}

void RecordFactory::StoreRecord(void *dst, Record *record) const {
  std::cerr << "< ---------------- RecordFactory::StoreRecord ---------------- >\n";
  // printf("dst: %p\n", dst);
  std::cerr << "dst: " << (size_t)dst << "\n";
  // std:: cerr << " < ----- record display ----- >\n";
  // record->Display();
  // TODO: 记录序列化
  // TIPS: 通过TableMeta可以读取各个字段的属性和长度，利用StoreField函数对于各个指针进行序列化。
  // TIPS: 友元可以直接访问私有变量
  // LAB 1 BEGIN
  // if (meta_->GetSize() != record->GetSize()) throw InvalidRecordSizeError(record->GetSize());
  size_t offset = 0;
  vector<int> var_len_idx;
  for(size_t i = 0; i < record->GetSize(); ++i) {
    auto field = record->GetField(i);
    if (field->GetType() == FieldType::STRING) {
      // 先跳过变长记录
      var_len_idx.push_back(i);
      continue;
    }
    auto col = meta_->cols_[i];
    std::cerr << "col.type: " << (int)col.type_ << " col.len: " << col.len_ << "\n";
    std::cerr << "field.type: " << (int)field->GetType() << " field.len: " << field->GetSize() << "\n";
    if (field->GetType() != col.type_) throw UnsupportFieldError();
    StoreField(static_cast<uint8_t *>(dst) + offset, field, col.type_, col.len_);
    offset += col.len_;
  }
  size_t var_data_start = offset + var_len_idx.size() * sizeof(int) * 2;
  size_t var_data_offset = 0;

  for (size_t i = 0; i < var_len_idx.size(); ++i) {
    // 先存 <offset: int, length: int>
    auto field = record->GetField(var_len_idx[i]);
    std::cerr << "field offset: " << var_data_start + var_data_offset << " field len: " << field->GetSize() << "\n";
    IntField *offset_field = new IntField(var_data_start + var_data_offset);
    IntField *length_field = new IntField(field->GetSize());
    var_data_offset += field->GetSize();
    std::cerr << "storing addr: " << (size_t)(static_cast<uint8_t *>(dst) + offset) << "\n";
    StoreField(static_cast<uint8_t *>(dst) + offset, offset_field, FieldType::INT, offset_field->GetSize());
    offset += offset_field->GetSize();
    StoreField(static_cast<uint8_t *>(dst) + offset, length_field, FieldType::INT, length_field->GetSize());
    offset += length_field->GetSize();
  }
  std::cerr << "before store var data, offset: " << offset << "\n";
  for (size_t i = 0; i < var_len_idx.size(); ++i) {
    // 再存 var data
    auto field = record->GetField(var_len_idx[i]);
    auto col = meta_->cols_[var_len_idx[i]];
    std::cerr << "col.type: " << (int)col.type_ << " col.len: " << col.len_ << "\n";
    std::cerr << "field.type: " << (int)field->GetType() << " field.len: " << field->GetSize() << "\n";
    if (field->GetType() != col.type_) throw UnsupportFieldError();
    StoreField(static_cast<uint8_t *>(dst) + offset, field, col.type_, field->GetSize());
    offset += field->GetSize();
  }
  // LAB 1 END
}

Rid RecordFactory::GetRid(Record *record) {
  // 读取隐藏列的RID信息
  PageID page_id = dynamic_cast<IntField *>(record->field_list_[record->GetSize() - PAGE_ID_OFFSET])->GetValue();
  SlotID slot_id = dynamic_cast<IntField *>(record->field_list_[record->GetSize() - SLOT_ID_OFFSET])->GetValue();
  return {page_id, slot_id};
}

void RecordFactory::SetRid(Record *record, Rid rid) {
  // 设置隐藏列的RID信息
  Field *page_field = new IntField(rid.page_no);
  Field *slot_field = new IntField(rid.slot_no);
  record->SetField(record->GetSize() - PAGE_ID_OFFSET, page_field);
  record->SetField(record->GetSize() - SLOT_ID_OFFSET, slot_field);
}

// TODO: 设置MVCC相关隐藏列的接口
// TIPS: 基础功能仅需要设置创建版本号和删除版本号
// LAB 3 BEGIN
// LAB 3 END

}  // namespace dbtrain
