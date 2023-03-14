#ifndef DBTRAIN_OPER_NODE_H
#define DBTRAIN_OPER_NODE_H

#include "../defines.h"
#include "../record/record.h"

namespace dbtrain {

class OperNode {
 public:
  OperNode(const vector<OperNode *> &childs);
  virtual ~OperNode();
  virtual RecordList Next() = 0;
  virtual double Cost();

  virtual void Display(int depth) const;

 protected:
  vector<OperNode *> childs_;
};

}  // namespace dbtrain

#endif
