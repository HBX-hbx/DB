#include "scan_node.h"
#include <iostream>
#include "../optim/stats_manager.h"
#include "../tx/tx_manager.h"

namespace dbtrain {

TableScanNode::TableScanNode(Table *table) : OperNode({}), table_(table) { cur_page_ = 0; }

TableScanNode::~TableScanNode() {}

RecordList TableScanNode::Next() {
  std::cerr << "< ------------- TableScanNode::Next() ------------- >\n";
  XID xid = TxManager::GetInstance().Get(std::this_thread::get_id());
  auto active_xids = TxManager::GetInstance().GetActiveSet(xid);
  RecordList outlist = {};
  while (outlist.size() == 0) {
    std::cerr << " -------------- in the loop -------------- \n";
    std::cerr << "xid: " << xid << "\n";
    if (cur_page_ >= table_->GetMeta().GetTableEnd()) return {};
    PageHandle page_handle = table_->GetPage(cur_page_);
    if (xid == INVALID_XID)
      outlist = page_handle.LoadRecords();
    else
      outlist = page_handle.LoadRecords(xid, active_xids);
    ++cur_page_;
  }
  std::cerr << "record list size: " << outlist.size() << "\n";
  return outlist;
}

double TableScanNode::Cost() { return StatsManager::GetInstance().GetRecordNum(table_->GetName(), 0); }

void TableScanNode::Display(int depth) const {
  for (int i = 0; i < depth; ++i) printf("\t");
  printf("Table Scan Node(%s):\n", table_->GetName().c_str());
  for (const auto &child : childs_) child->Display(depth + 1);
}

}  // namespace dbtrain
