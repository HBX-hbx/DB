#ifndef DBTRAIN_TX_MANAGER_H
#define DBTRAIN_TX_MANAGER_H

#include <map>
#include <mutex>
#include <set>

#include "../defines.h"

namespace dbtrain {

class TxManager {
 public:
  static TxManager &GetInstance();
  TxManager(const TxManager &) = delete;
  void operator=(const TxManager &) = delete;

  XID Push(TID tid);
  XID Pop(TID tid);
  XID Get(TID tid);

  void SetXID(XID xid);
  XID GetXID();

  // TIPS: 获取对应事务开始时仍在运行的事务编号集合
  // TIPS: 用于MVCC数据读取过程中过滤无效数据
  std::set<XID> GetActiveSet(XID xid);

 private:
  TxManager();

  std::set<XID> ActiveXIDs();
  std::mutex tx_lock_;
  std::map<TID, XID> tx_map_;
  std::map<XID, std::set<XID>> actset_map_;
  XID current_xid_;
};

}  // namespace dbtrain

#endif
