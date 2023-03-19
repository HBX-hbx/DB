#ifndef DBTRAIN_STRING_RECORD_H
#define DBTRAIN_STRING_RECORD_H

#include "field.h"

namespace dbtrain {

class StrField : public Field {
 public:
  StrField(const char *src, int size);
  StrField(int size);
  ~StrField();
  virtual void Load(const void *src, int s = 0) override;
  virtual void Store(void *dst, int s = 0) const override;
  virtual FieldType GetType() const override;
  virtual bool Equal(Field *field) const override;
  virtual bool Less(Field *field) const override;
  virtual bool Greater(Field *field) const override;
  virtual Field *Copy() const override;
  virtual std::string ToString() const override;
  string GetValue() const;
  virtual int GetSize() const override;

 protected:
  char *val_;
};

}  // namespace dbtrain

#endif
