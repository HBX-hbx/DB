#ifndef DBTRAIN_SCAN_NODE_H
#define DBTRAIN_SCAN_NODE_H

#include "conditions/condition.h"
#include "oper_node.h"
#include "../table/table.h"

namespace dbtrain {

class TableScanNode : public OperNode {
 public:
  TableScanNode(Table *table);
  ~TableScanNode();

  RecordList Next() override;
  double Cost() override;

  virtual void Display(int depth) const override;

 private:
  Table *table_;
  PageID cur_page_;
  Condition *cond_;
};

}  // namespace dbtrain

#endif
