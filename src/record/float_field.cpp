#include "float_field.h"
#include <iostream>
#include <sstream>

#include "../exception/record_exceptions.h"
#include "int_field.h"

namespace dbtrain {

FloatField::FloatField(double val) : val_{val} { size_ = 8; }

FloatField::FloatField() { size_ = 8; }

void FloatField::Load(const void *src, int s) {
  assert(s == size_);
  // std::cerr << "< ------------- FloatField::Load ------------- >\n";
  memcpy(&val_, src, s);
  // printf("load from: %p\n", src);
  // std::cerr << "val_: " << val_ << "\n";
}
void FloatField::Store(void *dst, int s) const {
  assert(s == size_);
  // std::cerr << "< ------------- FloatField::Store ------------- >\n";
  // printf("store to: %p\n", dst);
  // std::cerr << "val_: " << val_ << "\n";
  memcpy(dst, &val_, s);
}
FieldType FloatField::GetType() const { return FieldType::FLOAT; }
Field *FloatField::Copy() const { return new FloatField(val_); }

bool FloatField::Equal(Field *field) const {
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
bool FloatField::Less(Field *field) const {
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
bool FloatField::Greater(Field *field) const {
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

std::string FloatField::ToString() const {
  std::ostringstream oss;
  oss << val_;
  return oss.str();
}

double FloatField::GetValue() const { return val_; }

}  // namespace dbtrain
