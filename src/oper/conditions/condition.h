#ifndef DBTRAIN_CONDITION_H
#define DBTRAIN_CONDITION_H

#include "../../defines.h"
#include "../../record/record.h"

namespace dbtrain {

class Condition {
 public:
  Condition() = default;
  virtual ~Condition() = default;

  virtual bool Fit(Record *record) const = 0;

  virtual void Display() const = 0;

 private:
};

}  // namespace dbtrain

#endif
