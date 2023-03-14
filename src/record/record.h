#ifndef DBTRAIN_RECORD_H
#define DBTRAIN_RECORD_H

#include "../defines.h"
#include "field.h"

namespace dbtrain {

class Record {
 public:
  Record() = default;
  ~Record();

  Field *GetField(int idx) const;
  void SetField(int idx, Field *field);
  void PushBack(Field *field);
  Record *Copy() const;
  size_t GetSize() const;
  void Display() const;
  std::vector<std::string> ToString() const;

 private:
  vector<Field *> field_list_;

  friend class RecordFactory;
};

typedef Record Tuple;
typedef vector<Record *> RecordList;

}  // namespace dbtrain

#endif
