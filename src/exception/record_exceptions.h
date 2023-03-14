#ifndef DBTRAIN_RECORD_EXCEPTION_H
#define DBTRAIN_RECORD_EXCEPTION_H

#include "../exception/exceptions.h"

namespace dbtrain {

class RecordError : public DbError {
 public:
  explicit RecordError(const std::string &message) : DbError("(RE) " + message) {}
  ~RecordError() = default;
};

class UnsupportOpError : public RecordError {
 public:
  explicit UnsupportOpError() : RecordError("Operator not supported.") {}
  ~UnsupportOpError() = default;
};

class UnsupportFieldError : public RecordError {
 public:
  explicit UnsupportFieldError() : RecordError("Field type not supported.") {}
  ~UnsupportFieldError() = default;
};

}  // namespace dbtrain

#endif  // DBTRAIN_DBEXCEPTION_H
