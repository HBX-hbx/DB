#ifndef DBTRAIN_PAGE_H
#define DBTRAIN_PAGE_H

#include "../defines.h"

namespace dbtrain {

struct FilePageId {
  int fd;
  PageID page_no;

  bool operator==(const FilePageId &page_id) const { return (page_id.fd == fd) && (page_id.page_no == page_no); }
};

struct PageIdHash {
  size_t operator()(const FilePageId &page_id) const { return (page_id.fd << FD_BITS) | page_id.page_no; }
};

class Page {
  friend class BufferManager;

 public:
  Page() : is_dirty_(false) {}
  ~Page() = default;
  void SetDirty() { is_dirty_ = true; }
  uint8_t *GetData() { return data_; }
  FilePageId GetPageId() { return page_id_; }

 private:
  FilePageId page_id_;
  uint8_t data_[PAGE_SIZE];
  bool is_dirty_;
};

}  // namespace dbtrain

#endif  // DBTRAIN_PAGE_H
