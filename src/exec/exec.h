#ifndef DBTRAIN_EXECUTOR_H
#define DBTRAIN_EXECUTOR_H

#include "../defines.h"
#include "../oper/oper_node.h"

namespace dbtrain {

class Executor {
 public:
  Executor();
  Executor(OperNode *plan);
  ~Executor();

  RecordList RunNext();
  bool Finished() const;
  void AppendPlan(OperNode *plan);

  void Begin();
  void Commit();
  void Abort();

 private:
  vector<OperNode *> plans_;
  int next_;
};

};  // namespace dbtrain

#endif