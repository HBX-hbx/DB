#include "str_field.h"

#include "../exception/record_exceptions.h"

namespace dbtrain {

StrField::StrField(const char *src, int size) {
  size_ = size;
  val_ = new char[size_ + 1];
  val_[size_] = '\0';
  memcpy(val_, src, size_);
}

StrField::StrField(int size) {
  size_ = size;
  val_ = new char[size_ + 1];
  val_[size_] = '\0';
  memset(val_, 0, size_);
}

StrField::~StrField() { delete[] val_; }

void StrField::Load(const void *src, int s) { memcpy(val_, src, s); }

void StrField::Store(void *dst, int s) const {
  if (s <= size_ + 1) {
    memcpy(dst, val_, s);
  } else {
    memcpy(dst, val_, size_ + 1);
  }
}
FieldType StrField::GetType() const { return FieldType::STRING; }

Field *StrField::Copy() const { return new StrField(val_, size_); }

bool StrField::Equal(Field *field) const {
  if (field->GetType() == FieldType::STRING) {
    auto val = dynamic_cast<StrField *>(field)->GetValue();
    bool res = string(val_) == val;
    return res;
  } else {
    throw UnsupportOpError();
  }
}
bool StrField::Less(Field *field) const {
  if (field->GetType() == FieldType::STRING) {
    auto val = dynamic_cast<StrField *>(field)->GetValue();
    bool res = string(val_) < val;
    return res;
  } else {
    throw UnsupportOpError();
  }
}
bool StrField::Greater(Field *field) const {
  if (field->GetType() == FieldType::STRING) {
    auto val = dynamic_cast<StrField *>(field)->GetValue();
    bool res = string(val_) > val;
    return res;
  } else {
    throw UnsupportOpError();
  }
}

std::string StrField::ToString() const { return val_; }

string StrField::GetValue() const { return val_; }

}  // namespace dbtrain
