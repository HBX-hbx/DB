#ifndef DBTRAIN_LOCK_MANAGER_H
#define DBTRAIN_LOCK_MANAGER_H

#include <map>
#include <mutex>
#include <shared_mutex>

#include "../defines.h"

namespace dbtrain {

class LockManager {
 public:
  ~LockManager();
  static LockManager &GetInstance();
  LockManager(const LockManager &) = delete;
  void operator=(const LockManager &) = delete;

  // 排他锁
  void Lock(string lname);
  void Unlock(string lname);
  // 共享锁
  void LockShared(string lname);
  void UnlockShared(string lname);

 private:
  LockManager();
  std::shared_mutex *GetLock(string lname);
  std::mutex map_lock_;
  std::map<string, std::shared_mutex *> lock_map_;
};

}  // namespace dbtrain

#endif