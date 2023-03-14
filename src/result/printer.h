#ifndef DBTRAIN_PRINTER_H
#define DBTRAIN_PRINTER_H

#include "../result/result.h"

namespace dbtrain {

class Printer {
 public:
  virtual ~Printer() = default;
  virtual void Print(const Result *result) = 0;
};

}  // namespace dbtrain

#endif  // DBTRAIN_PRINTER_H
