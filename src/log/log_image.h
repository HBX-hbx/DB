#ifndef DBTRAIN_LOG_IMAGE_H
#define DBTRAIN_LOG_IMAGE_H
#include "../defines.h"

namespace dbtrain {

class PhysiologicalImage {
  enum class LogOpType { UPDATE, DELETE, INSERT };

 public:
  PhysiologicalImage();
  ~PhysiologicalImage();

  void Load(const Byte *src);
  size_t Store(Byte *dst);

  size_t GetLength() const;

 private:
  string table_name_;
  PageID page_id_;
  SlotID slot_id_;
  LogOpType op_type_;
  Byte *old_val_, *new_val_;
  size_t old_len_, new_len_;

  friend class LogFactory;
  friend class UpdateLog;
};

}  // namespace dbtrain

#endif