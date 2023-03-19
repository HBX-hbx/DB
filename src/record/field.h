#ifndef DBTRAIN_FIELD_H
#define DBTRAIN_FIELD_H

#include "../defines.h"

namespace dbtrain {

class Field {
 public:
  Field() = default;
  virtual ~Field() = default;

  virtual void Load(const void *src, int s = 0) = 0;
  virtual void Store(void *dst, int s = 0) const = 0;
  virtual FieldType GetType() const = 0;
  virtual Field *Copy() const = 0;
  virtual bool Equal(Field *field) const = 0;
  virtual bool Less(Field *field) const = 0;
  virtual bool Greater(Field *field) const = 0;
  virtual std::string ToString() const = 0;
  virtual int GetSize() const;

 protected:
  int size_;
};

}  // namespace dbtrain

#endif
