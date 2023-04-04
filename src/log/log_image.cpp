#include "log_image.h"
#include <iostream>

#include "../system/system_manager.h"

namespace dbtrain {

PhysiologicalImage::PhysiologicalImage() : old_val_(nullptr), new_val_(nullptr) {}

PhysiologicalImage::~PhysiologicalImage() {
  if (old_val_ != nullptr) delete[] old_val_;
  if (new_val_ != nullptr) delete[] new_val_;
}

void PhysiologicalImage::Load(const Byte *src) {
  // TODO: Log Image反序列化
  // TIPS: 根据操作类型区分
  // LAB 2 BEGIN
  size_t offset = 0;
  // string table_name_
  size_t len_of_string = 0; // store '\0'
  memcpy(&len_of_string, src + offset, sizeof(size_t));
  offset += sizeof(size_t);

  char* table_name = new char[len_of_string];
  memcpy(table_name, src + offset, len_of_string);
  offset += len_of_string;
  table_name_.assign(table_name);
  // PageID page_id_
  memcpy(&page_id_, src + offset, sizeof(PageID));
  offset += sizeof(PageID);
  // SlotID slot_id_
  memcpy(&slot_id_, src + offset, sizeof(SlotID));
  offset += sizeof(SlotID);
  // LogOpType op_type_
  memcpy(&op_type_, src + offset, sizeof(LogOpType));
  offset += sizeof(LogOpType);

  if (op_type_ == LogOpType::DELETE || op_type_ == LogOpType::UPDATE) {
    // size_t old_len_
    memcpy(&old_len_, src + offset, sizeof(size_t));
    offset += sizeof(size_t);
    // Byte *old_val_
    old_val_ = new Byte[old_len_];
    memcpy(old_val_, src + offset, old_len_);
    offset += old_len_;
  }
  
  if (op_type_ == LogOpType::INSERT || op_type_ == LogOpType::UPDATE) {
    // size_t new_len_
    memcpy(&new_len_, src + offset, sizeof(size_t));
    offset += sizeof(size_t);
    // Byte *new_val_
    new_val_ = new Byte[new_len_];
    memcpy(new_val_, src + offset, new_len_);
    offset += new_len_;
  }
  // LAB 2 END
}

size_t PhysiologicalImage::Store(Byte *dst) {
  // TODO: Log Image序列化
  // TIPS: 根据操作类型区分，返回Store的数据长度
  // LAB 2 BEGIN
  size_t offset = 0;
  // string table_name_
  size_t len_of_string = table_name_.size() + 1; // store '\0'
  memcpy(dst + offset, &len_of_string, sizeof(size_t));
  offset += sizeof(size_t);
  memcpy(dst + offset, table_name_.c_str(), len_of_string);
  offset += len_of_string;
  // PageID page_id_
  memcpy(dst + offset, &page_id_, sizeof(PageID));
  offset += sizeof(PageID);
  // SlotID slot_id_
  memcpy(dst + offset, &slot_id_, sizeof(SlotID));
  offset += sizeof(SlotID);
  // LogOpType op_type_
  memcpy(dst + offset, &op_type_, sizeof(LogOpType));
  offset += sizeof(LogOpType);

  if (op_type_ == LogOpType::DELETE || op_type_ == LogOpType::UPDATE) {
    // size_t old_len_
    memcpy(dst + offset, &old_len_, sizeof(size_t));
    offset += sizeof(size_t);
    // Byte *old_val_
    memcpy(dst + offset, old_val_, old_len_);
    offset += old_len_;
  }

  if (op_type_ == LogOpType::INSERT || op_type_ == LogOpType::UPDATE) {
    // size_t new_len_
    memcpy(dst + offset, &new_len_, sizeof(size_t));
    offset += sizeof(size_t);
    // Byte *new_val_
    memcpy(dst + offset, new_val_, new_len_);
    offset += new_len_;
  }
  
  return offset;
  // LAB 2 END
}

// get the length of the image(includes the variable info like len_of_string)
size_t PhysiologicalImage::GetLength() const {
  // TODO: 获取Log Image的长度
  // TIPS: 根据操作类型区分
  // LAB 2 BEGIN
  size_t offset = 0;
  // string table_name_
  size_t len_of_string = table_name_.size() + 1; // store '\0'
  offset += sizeof(size_t);
  offset += len_of_string;
  // PageID page_id_
  offset += sizeof(PageID);
  // SlotID slot_id_
  offset += sizeof(SlotID);
  // LogOpType op_type_
  offset += sizeof(LogOpType);

  if (op_type_ == LogOpType::DELETE || op_type_ == LogOpType::UPDATE) {
    // size_t old_len_
    offset += sizeof(size_t);
    // Byte *old_val_
    offset += old_len_;
  }

  if (op_type_ == LogOpType::INSERT || op_type_ == LogOpType::UPDATE) {
    // size_t new_len_
    offset += sizeof(size_t);
    // Byte *new_val_
    offset += new_len_;
  }
  
  return offset;
  // LAB 2 END
}

}  // namespace dbtrain
