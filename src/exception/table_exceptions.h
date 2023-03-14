#ifndef DBTRAIN_RECORD_EXCEPTION_H
#define DBTRAIN_RECORD_EXCEPTION_H

#include "../exception/exceptions.h"

namespace dbtrain {

class TableError : public DbError {
 public:
  explicit TableError(const std::string &message) : DbError("(TE) " + message) {}
  ~TableError() = default;
};

class ColumnNotExistsError : public TableError {
 public:
  explicit ColumnNotExistsError(const std::string &table_name, const std::string &col_name)
      : TableError(table_name + " does not have column " + col_name + ".") {}
  ~ColumnNotExistsError() = default;
};

class TableNotJoinedError : public TableError {
 public:
  explicit TableNotJoinedError(const std::string &table_name) : TableError(table_name + " not joined.") {}
  ~TableNotJoinedError() = default;
};

}  // namespace dbtrain

#endif
