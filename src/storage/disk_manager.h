#ifndef DBTRAIN_DISK_MANAGER_H
#define DBTRAIN_DISK_MANAGER_H

#include <dirent.h>
#include <limits.h>
#include <unistd.h>

#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "../defines.h"

namespace dbtrain {

class DiskManager {
  friend class BufferManager;

 public:
  DiskManager(const DiskManager &) = delete;
  void operator=(const DiskManager &) = delete;
  static DiskManager &GetInstance();

  void CreateDirectory(const std::string &path);
  void ChangeDirectory(const std::string &path);
  void DeleteDirectory(const std::string &path);
  void ListDirectories(const std::string &path, std::vector<std::string> &dirs);
  void ListTables(const std::string &path, std::vector<std::string> &files);
  void CreateFile(const std::string &path);
  void DeleteFile(const std::string &path);
  size_t FileSize(const std::string &path);
  int OpenFile(const std::string &path);
  void CloseFile(int fd);

  void ReadRaw(int fd, Byte *data, size_t size);
  void ReadRaw(int fd, Byte *data, size_t size, size_t offset);
  void AppendRaw(int fd, const Byte *data, size_t size);
  void WriteRaw(int fd, const Byte *data, size_t size);
  void FlushFile(int fd);

  size_t ReadIndex(int fs, size_t idx);

 private:
  DiskManager();

  void ReadPage(int fd, int page_id, Byte *page_data);
  void WritePage(int fd, int page_id, const Byte *page_data);

  bool FileExists(const std::string &path);
  bool DirectoryExists(const std::string &path);

  std::vector<std::string> database_names_;
  std::unordered_map<std::string, int> path2fd_;
  std::unordered_map<int, std::string> fd2path_;
  char db_dir[PATH_MAX];
};

}  // namespace dbtrain

#endif  // DBTRAIN_DISK_MANAGER_H
