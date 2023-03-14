#ifndef DBTRAIN_BUFFER_MANAGER_H
#define DBTRAIN_BUFFER_MANAGER_H

#include <list>

#include "../defines.h"
#include "../log/log_manager.h"
#include "../storage/disk_manager.h"
#include "../storage/page.h"

namespace dbtrain {

class BufferManager {
 public:
  BufferManager(const BufferManager &) = delete;
  void operator=(const BufferManager &) = delete;
  ~BufferManager();
  static BufferManager &GetInstance();
  Page *AllocPage(int fd, PageID page_no);
  Page *GetPage(int fd, PageID page_no);
  void Clear();  // Used for crash test
  void FlushFile(int fd);
  void FlushAll();
  void WriteBack(Page *page);

 private:
  BufferManager();
  Page *FetchPage(int fd, PageID page_no);
  bool PageInCache(const FilePageId file_page_id);
  void FlushPage(Page *page);
  void Access(int frame_no);

  DiskManager &disk_manager_;
  LogManager &log_manager_;
  Page frames_[BUFFER_SIZE];
  std::unordered_map<FilePageId, int, PageIdHash> hashmap_;

  std::list<int> busy_list_;
  std::list<int> free_list_;
};

}  // namespace dbtrain

#endif  // DBTRAIN_BUFFER_MANAGER_H
