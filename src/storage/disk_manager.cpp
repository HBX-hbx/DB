#include "disk_manager.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>

#include "../defines.h"
#include "../exception/exceptions.h"
#include "../storage/buffer_manager.h"

namespace dbtrain {

DiskManager::DiskManager() {
  if (!DirectoryExists(BASE_PATH)) {
    CreateDirectory(BASE_PATH);
  }
  ChangeDirectory(BASE_PATH);
  getcwd(db_dir, sizeof(db_dir));
}

DiskManager &DiskManager::GetInstance() {
  static DiskManager disk_manager;
  return disk_manager;
}

void DiskManager::ListDirectories(const std::string &path, std::vector<std::string> &dirs) {
  DIR *dir;
  struct dirent *ent;
  if (!DirectoryExists(path)) {
    throw FileNotExistsError(path);
  } else if ((dir = opendir(path.c_str())) != nullptr) {
    while ((ent = readdir(dir)) != nullptr) {
      if (ent->d_name[0] != '.' && ent->d_type == DT_DIR) {
        dirs.push_back(ent->d_name);
      }
    }
    closedir(dir);
  } else {
    std::cerr << "Error in DiskManager::ListDirectories\n";
    throw UnknownError();
  }
}

bool Endswith(const std::string &str, const std::string &suffix) {
  return str.size() >= suffix.size() && str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

void DiskManager::ListTables(const std::string &path, std::vector<std::string> &files) {
  DIR *dir;
  struct dirent *ent;
  if (!DirectoryExists(path)) {
    throw FileNotExistsError(path);
  } else if ((dir = opendir(path.c_str())) != nullptr) {
    while ((ent = readdir(dir)) != nullptr) {
      if (ent->d_type == DT_REG && Endswith(ent->d_name, DB_META_SUFFIX)) {
        std::string filename = ent->d_name;
        files.push_back(filename.substr(0, filename.size() - DB_META_SUFFIX.size()));
      }
    }
    closedir(dir);
  } else {
    std::cerr << "Error in DiskManager::ListTables\n";
    throw UnknownError();
  }
}

void DiskManager::CreateDirectory(const std::string &path) {
  if (DirectoryExists(path)) {
    throw FileExistsError(path);
  }
  std::string cmd = "mkdir " + path;
  if (system(cmd.c_str()) != 0) {
    std::cerr << "Error in DiskManager::CreateDirectory\n";
    throw UnknownError();
  }
}

void DiskManager::ChangeDirectory(const std::string &path) {
  if (!DirectoryExists(path)) {
    throw FileNotExistsError(path);
  }
  if (chdir(path.c_str()) != 0) {
    std::cerr << "Error in DiskManager::ChangeDirectory\n";
    throw UnknownError();
  }
}

void DiskManager::DeleteDirectory(const std::string &path) {
  if (!DirectoryExists(path)) {
    throw FileNotExistsError(path);
  }
  std::string cmd = "rm -r " + path;
  if (system(cmd.c_str()) != 0) {
    std::cerr << "Error in DiskManager::DeleteDirectory\n";
    throw UnknownError();
  }
}

void DiskManager::CreateFile(const std::string &path) {
  if (FileExists(path)) {
    throw FileExistsError(path);
  }
  int fd = open(path.c_str(), O_CREAT, S_IRUSR | S_IWUSR);
  if (fd < 0) {
    std::cerr << "Error in DiskManager::CreateFile: fd < 0\n";
    throw UnknownError();
  }
  if (close(fd) != 0) {
    std::cerr << "Error in DiskManager::CreateFile: close(fd) != 0\n";
    throw UnknownError();
  }
}

void DiskManager::DeleteFile(const std::string &path) {
  if (!FileExists(path)) {
    throw FileNotExistsError(path);
  }
  if (path2fd_.count(path)) {
    throw FileNotClosedError(path);
  }
  if (unlink(path.c_str()) != 0) {
    std::cerr << "Error in DiskManager::DeleteFile\n";
    throw UnknownError();
  }
}

size_t DiskManager::FileSize(const std::string &path) {
  if (!FileExists(path)) {
    throw FileNotExistsError(path);
  }
  struct stat st;
  if (stat(path.c_str(), &st) != 0) {
    std::cerr << "Error in DiskManager::FileSize\n";
    throw UnknownError();
  }
  return st.st_size;
}

int DiskManager::OpenFile(const std::string &path) {
  if (!FileExists(path)) {
    throw FileNotExistsError(path);
  }
  if (path2fd_.count(path)) {
    throw FileNotClosedError(path);
  }
  int fd = open(path.c_str(), O_RDWR);
  if (fd < 0) {
    std::cerr << "Error in DiskManager::OpenFile\n";
    throw UnknownError();
  }
  path2fd_[path] = fd;
  fd2path_[fd] = path;
  return fd;
}

void DiskManager::CloseFile(int fd) {
  if (!fd2path_.count(fd)) {
    throw FileNotOpenError(fd);
  }
  BufferManager::GetInstance().FlushFile(fd);
  path2fd_.erase(fd2path_[fd]);
  fd2path_.erase(fd);
  if (close(fd) != 0) {
    std::cerr << "Error in DiskManager::CloseFile\n";
    throw UnknownError();
  }
}

void DiskManager::ReadPage(int fd, int page_id, Byte *page_data) {
  lseek(fd, page_id * PAGE_SIZE, SEEK_SET);
  ssize_t bytes_read = read(fd, page_data, PAGE_SIZE);
  if (bytes_read != PAGE_SIZE) {
    std::cerr << "Error in DiskManager::ReadPage\n";
    throw UnknownError();
  }
}

void DiskManager::WritePage(int fd, int page_id, const Byte *page_data) {
  lseek(fd, page_id * PAGE_SIZE, SEEK_SET);
  ssize_t bytes_write = write(fd, page_data, PAGE_SIZE);
  if (bytes_write != PAGE_SIZE) {
    std::cerr << "Error in DiskManager::WritePage\n";
    throw UnknownError();
  }
}

void DiskManager::ReadRaw(int fd, Byte *data, size_t size) {
  lseek(fd, 0, SEEK_SET);
  ssize_t bytes_read = read(fd, data, size);
  if (bytes_read != size) {
    std::cerr << "Error in DiskManager::ReadRaw 1\n";
    throw UnknownError();
  }
}

void DiskManager::ReadRaw(int fd, Byte *data, size_t size, size_t offset) {
  lseek(fd, offset, SEEK_SET);
  ssize_t bytes_read = read(fd, data, size);
  if (bytes_read != size) {
    std::cerr << "Error in DiskManager::ReadRaw 2\n";
    throw UnknownError();
  }
}

void DiskManager::AppendRaw(int fd, const Byte *data, size_t size) {
  lseek(fd, 0, SEEK_END);
  ssize_t bytes_write = write(fd, data, size);
  if (bytes_write != size) {
    std::cerr << "Error in DiskManager::AppendRaw\n";
    throw UnknownError();
  }
}

void DiskManager::WriteRaw(int fd, const Byte *data, size_t size) {
  lseek(fd, 0, SEEK_SET);
  ssize_t bytes_write = write(fd, data, size);
  if (bytes_write != size) {
    std::cerr << "Error in DiskManager::WriteRaw\n";
    throw UnknownError();
  }
}

size_t DiskManager::ReadIndex(int fd, size_t idx) {
  lseek(fd, idx * sizeof(size_t), SEEK_SET);
  size_t idx_val = 0;
  ssize_t bytes_write = read(fd, &idx_val, sizeof(size_t));
  if (bytes_write != sizeof(size_t)) {
    std::cerr << "Error in DiskManager::ReadIndex\n";
    throw UnknownError();
  }
  return idx_val;
}

void DiskManager::FlushFile(int fd) {
  if (fsync(fd) != 0) {
    std::cerr << "Error in DiskManager::FlushFile\n";
    throw UnknownError();
  }
}

bool DiskManager::FileExists(const std::string &path) {
  struct stat buffer;
  return (stat(path.c_str(), &buffer) == 0 && S_ISREG(buffer.st_mode));
}

bool DiskManager::DirectoryExists(const std::string &path) {
  struct stat buffer;
  return (stat(path.c_str(), &buffer) == 0 && S_ISDIR(buffer.st_mode));
}

}  // namespace dbtrain
