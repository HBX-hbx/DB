#include "int_field.h"

#include "../exception/record_exceptions.h"
#include "float_field.h"

namespace dbtrain {

IntField::IntField(int val) : val_{val} { size_ = 4; }

IntField::IntField() { size_ = 4; }

void IntField::Load(const void *src, int s) {
  assert(s == size_);
  memcpy(&val_, src, s);
}
void IntField::Store(void *dst, int s) const {
  assert(s == size_);
  memcpy(dst, &val_, s);
}
FieldType IntField::GetType() const { return FieldType::INT; }
Field *IntField::Copy() const { return new IntField(val_); }

bool IntField::Equal(Field *field) const {
  if (field->GetType() == FieldType::INT) {
    int val = dynamic_cast<IntField *>(field)->GetValue();
    return val_ == val;
  } else if (field->GetType() == FieldType::FLOAT) {
    double val = dynamic_cast<FloatField *>(field)->GetValue();
    return val_ == val;
  } else {
    throw UnsupportOpError();
  }
}
bool IntField::Less(Field *field) const {
  if (field->GetType() == FieldType::INT) {
    int val = dynamic_cast<IntField *>(field)->GetValue();
    return val_ < val;
  } else if (field->GetType() == FieldType::FLOAT) {
    double val = dynamic_cast<FloatField *>(field)->GetValue();
    return val_ < val;
  } else {
    throw UnsupportOpError();
  }
}
bool IntField::Greater(Field *field) const {
  if (field->GetType() == FieldType::INT) {
    int val = dynamic_cast<IntField *>(field)->GetValue();
    return val_ > val;
  } else if (field->GetType() == FieldType::FLOAT) {
    double val = dynamic_cast<FloatField *>(field)->GetValue();
    return val_ > val;
  } else {
    throw UnsupportOpError();
  }
}

std::string IntField::ToString() const { return std::to_string(val_); }

int IntField::GetValue() const { return val_; }

}  // namespace dbtrain
