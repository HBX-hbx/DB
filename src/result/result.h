#ifndef DBTRAIN_RESULT_H
#define DBTRAIN_RESULT_H

#include <string>
#include <vector>

#include "../record/record.h"

namespace dbtrain {

class Result {
  friend class RawPrinter;
  friend class TablePrinter;

 public:
  Result() = default;
  ~Result();
  Result(const std::vector<std::string> header);
  Result(const std::vector<std::string> header, const RecordList &records);
  Result(const RecordList &);
  void InsertRecords(const RecordList &records);
  void Display();

 private:
  std::vector<std::string> header_;
  RecordList records_;
};

}  // namespace dbtrain

#endif  // DBTRAIN_RESULT_H
