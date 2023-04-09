#include "tx_manager.h"

#include "log/log_manager.h"

namespace dbtrain {

TxManager &TxManager::GetInstance() {
  static TxManager tx_manager;
  return tx_manager;
}

TxManager::TxManager() {}

XID TxManager::Push(TID tid) {
  tx_lock_.lock();
  XID xid = current_xid_++;
  actset_map_[xid] = ActiveXIDs();
  tx_map_[tid] = xid;
  tx_lock_.unlock();
  return xid;
}

XID TxManager::Pop(TID tid) {

  tx_lock_.lock();
  XID xid = tx_map_[tid];
  tx_map_.erase(tid);
  actset_map_.erase(xid);
  tx_lock_.unlock();
  return xid;
}

XID TxManager::Get(TID tid) {
  tx_lock_.lock();
  if (tx_map_.find(tid) == tx_map_.end()) {
    tx_lock_.unlock();
    return INVALID_XID;
  }
  auto res = tx_map_[tid];
  tx_lock_.unlock();
  return res;
}

std::set<XID> TxManager::ActiveXIDs() {
  auto active_xids = std::set<XID>();
  for (const auto &pair : tx_map_) active_xids.insert(pair.second);
  return active_xids;
}

std::set<XID> TxManager::GetActiveSet(XID xid) {
  if (xid == INVALID_XID) return {};
  tx_lock_.lock();
  auto actset = actset_map_[xid];
  tx_lock_.unlock();
  return actset;
}

void TxManager::SetXID(XID xid) { current_xid_ = xid; }
XID TxManager::GetXID() { return current_xid_; }

}  // namespace dbtrain
