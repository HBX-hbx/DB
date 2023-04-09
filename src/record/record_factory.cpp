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
  // std::cerr << "< ---------------- RecordFactory::StoreField ---------------- >\n";
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
  // std::cerr << "< ---------------- RecordFactory::LoadRecord ---------------- >\n";
  // printf("src: %p\n", src);
  // std::cerr << "src: " << (size_t)src << "\n";
  // TODO: 记录反序列化
  // TIPS: 通过TableMeta可以读取各个字段的属性和长度，利用LoadField函数建立各个字段对应的Field指针。
  // LAB 1 BEGIN
  Record* record = new Record();
  size_t offset = 0;
  for (auto &col: meta_->cols_) {
    Field* field = LoadField(static_cast<const uint8_t *>(src) + offset, col.type_, col.len_);
    offset += col.len_;
    record->PushBack(field);
  }
  return record;
  // LAB 1 END
}

void RecordFactory::StoreRecord(void *dst, Record *record) const {
  // std::cerr << "< ---------------- RecordFactory::StoreRecord ---------------- >\n";
  // printf("dst: %p\n", dst);
  // std::cerr << "dst: " << (size_t)dst << "\n";
  // TODO: 记录序列化
  // TIPS: 通过TableMeta可以读取各个字段的属性和长度，利用StoreField函数对于各个指针进行序列化。
  // TIPS: 友元可以直接访问私有变量
  // LAB 1 BEGIN
  // if (meta_->GetSize() != record->GetSize()) throw InvalidRecordSizeError(record->GetSize());
  size_t offset = 0;
  for(size_t i = 0; i < record->GetSize(); ++i) {
    auto field = record->GetField(i);
    auto col = meta_->cols_[i];
    // std::cerr << "col.type: " << (int)col.type_ << " col.len: " << col.len_ << "\n";
    // std::cerr << "field.type: " << (int)field->GetType() << " field.len: " << field->GetSize() << "\n";
    if (field->GetType() != col.type_) throw UnsupportFieldError();
    StoreField(static_cast<uint8_t *>(dst) + offset, field, col.type_, col.len_);
    offset += col.len_;
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
