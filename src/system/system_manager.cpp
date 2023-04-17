#include "system_manager.h"

#include <algorithm>
#include <iostream>

#include "../defines.h"
#include "../exception/exceptions.h"
#include "../log/log_factory.h"
#include "../log/log_manager.h"
#include "../optim/stats_manager.h"
#include "../record/fields.h"
#include "../record/record_factory.h"
#include "../table/hidden.h"

namespace dbtrain {

SystemManager::SystemManager() : disk_manager_(DiskManager::GetInstance()), log_manager_(LogManager::GetInstance()) {
  disk_manager_.ListDirectories(".", db_names_);
}

SystemManager &SystemManager::GetInstance() {
  static SystemManager system_manager;
  return system_manager;
}

void SystemManager::UsingTest() {
  if (using_db_.empty()) {
    throw NoUsingDatabaseError();
  }
}

Result SystemManager::ShowDatabases() {
  RecordList records;
  for (const auto &db_name : db_names_) {
    Record *record = new Record();
    record->PushBack(new StrField(db_name.c_str(), db_name.size()));
    records.push_back(record);
  }
  return Result(std::vector<std::string>{"Database"}, records);
}

Result SystemManager::CreateDatabase(const std::string &db_name) {
  if (std::find(db_names_.begin(), db_names_.end(), db_name) != db_names_.end()) {
    throw DatabaseExistsError(db_name);
  }
  if (!using_db_.empty()) {
    disk_manager_.CreateDirectory("../" + db_name);
  } else {
    disk_manager_.CreateDirectory(db_name);
  }
  db_names_.push_back(db_name);
  InitLog(db_name);
  return Result(std::vector<std::string>{"SUCCESS"});
}

Result SystemManager::UseDatabase(const std::string &db_name) {
  std::cerr << "< ----- SystemManager::UseDatabase ----- >\n";
  if (db_name != using_db_) {
    std::cerr << "< ----- 1 ----- >\n";
    if (std::find(db_names_.begin(), db_names_.end(), db_name) == db_names_.end()) {
      throw DatabaseNotExistsError(db_name);
    }
    std::cerr << "< ----- 2 ----- >\n";
    if (!using_db_.empty()) {
      CloseDatabase(using_db_);
    }
    std::cerr << "< ----- 3 ----- >\n";
    if (using_db_.empty()) {
      disk_manager_.ChangeDirectory(db_name);
    } else {
      disk_manager_.ChangeDirectory("../" + db_name);
    }
    std::cerr << "< ----- 4 ----- >\n";
    using_db_ = db_name;
    tables_.clear();
    std::vector<std::string> table_names;
    disk_manager_.ListTables(".", table_names);
    std::cerr << "< ----- 5 ----- >\n";
    for (auto &table_name : table_names) {
      int meta_fd = disk_manager_.OpenFile(table_name + DB_META_SUFFIX);
      table2metafd_[table_name] = meta_fd;
      int data_fd = disk_manager_.OpenFile(table_name + DB_DATA_SUFFIX);
      table2datafd_[table_name] = data_fd;
      // 反向映射
      fd2table_[meta_fd] = table_name;
      fd2table_[data_fd] = table_name;

      tables_[table_name] = new Table(table_name, meta_fd, data_fd);
    }
    // 载入日志，准备开始
    LoadLogManager();
  }
  return Result(std::vector<std::string>{"SUCCESS"});
}

Result SystemManager::DropDatabase(const std::string &db_name, bool if_exists) {
  if (using_db_ == db_name) {
    throw DropUsingDatabaseError();
  }
  if (std::find(db_names_.begin(), db_names_.end(), db_name) == db_names_.end()) {
    if (!if_exists) {
      throw DatabaseNotExistsError(db_name);
    }
  } else {
    if (!using_db_.empty()) {
      disk_manager_.DeleteDirectory("../" + db_name);
    } else {
      disk_manager_.DeleteDirectory(db_name);
    }
    db_names_.erase(std::remove(db_names_.begin(), db_names_.end(), db_name), db_names_.end());
  }
  return Result(std::vector<std::string>{"SUCCESS"});
}

void SystemManager::CloseDatabase() {
  if (using_db_.empty()) return;
  BufferManager::GetInstance().FlushAll();
  CloseDatabase(using_db_);
}

void SystemManager::CloseDatabase(const std::string &db_name) {
  // LAB 2: 日志写回
  StoreLogManager();

  // 清除缓存
  for (const auto &table : tables_) {
    delete table.second;
    disk_manager_.CloseFile(table2metafd_[table.first]);
    disk_manager_.CloseFile(table2datafd_[table.first]);
    // 反向映射
    fd2table_.erase(table2datafd_[table.first]);
    fd2table_.erase(table2metafd_[table.first]);

    table2metafd_.erase(table.first);
    table2datafd_.erase(table.first);
  }
  disk_manager_.CloseFile(log_data_fd_);
  disk_manager_.CloseFile(log_index_fd_);
}

void SystemManager::Crash() {
  // 清除缓存
  BufferManager::GetInstance().Clear();
  LogManager::GetInstance().Init();
  for (const auto &table : tables_) {
    disk_manager_.CloseFile(table2metafd_[table.first]);
    disk_manager_.CloseFile(table2datafd_[table.first]);
    // 反向映射
    fd2table_.erase(table2datafd_[table.first]);
    fd2table_.erase(table2metafd_[table.first]);

    table2metafd_.erase(table.first);
    table2datafd_.erase(table.first);
  }
  tables_.clear();
  disk_manager_.CloseFile(log_data_fd_);
  disk_manager_.CloseFile(log_index_fd_);
  if (!using_db_.empty()) {
    using_db_ = "";
    disk_manager_.ChangeDirectory("..");
  }
  log_index_fd_ = -1;
  log_data_fd_ = -1;
}

Table *SystemManager::GetTable(const std::string &table_name) {
  if (using_db_.empty()) {
    throw NoUsingDatabaseError();
  }
  auto table = tables_.find(table_name);
  if (table == tables_.end()) {
    throw TableNotExistsError(table_name);
  }
  return table->second;
}

Result SystemManager::CreateTable(const std::string &table_name, const std::vector<Column> &columns) {
  if (using_db_.empty()) {
    throw NoUsingDatabaseError();
  }
  if (tables_.find(table_name) != tables_.end()) {
    throw TableExistsError(table_name);
  }

  disk_manager_.CreateFile(table_name + DB_META_SUFFIX);
  int meta_fd = disk_manager_.OpenFile(table_name + DB_META_SUFFIX);
  table2metafd_[table_name] = meta_fd;
  disk_manager_.CreateFile(table_name + DB_DATA_SUFFIX);
  int data_fd = disk_manager_.OpenFile(table_name + DB_DATA_SUFFIX);
  table2datafd_[table_name] = data_fd;
  // 反向映射
  fd2table_[meta_fd] = table_name;
  fd2table_[data_fd] = table_name;

  Table *table = new Table(table_name, meta_fd, data_fd, columns);
  tables_[table_name] = table;
  return Result(std::vector<std::string>{"SUCCESS"});
}

Result SystemManager::ShowTables() {
  if (using_db_.empty()) {
    throw NoUsingDatabaseError();
  }

  RecordList records;
  for (auto &table : tables_) {
    Record *record = new Record();
    record->PushBack(new StrField(table.first.c_str(), table.first.size()));
    records.push_back(record);
  }
  return Result(std::vector<std::string>{"Tables"}, records);
}

Result SystemManager::DescTable(const std::string &table_name) {
  if (using_db_.empty()) {
    throw NoUsingDatabaseError();
  }
  return GetTable(table_name)->Desc();
}

Result SystemManager::DropTable(const std::string &table_name) {
  if (using_db_.empty()) {
    throw NoUsingDatabaseError();
  }
  if (tables_.find(table_name) == tables_.end()) {
    throw TableNotExistsError(table_name);
  }
  delete tables_[table_name];
  disk_manager_.CloseFile(table2metafd_[table_name]);
  disk_manager_.CloseFile(table2datafd_[table_name]);
  // 反向映射
  fd2table_.erase(table2datafd_[table_name]);
  fd2table_.erase(table2metafd_[table_name]);

  disk_manager_.DeleteFile(table_name + DB_META_SUFFIX);
  disk_manager_.DeleteFile(table_name + DB_DATA_SUFFIX);
  tables_.erase(table_name);
  table2metafd_.erase(table_name);
  table2datafd_.erase(table_name);
  return Result(std::vector<std::string>{"SUCCESS"});
}

void SystemManager::Analyze() {
  if (using_db_.empty()) {
    throw NoUsingDatabaseError();
  }
  for (const auto &table : tables_) {
    int col_size = table.second->GetColumnSize();
    for (int i = 0; i < col_size - hidden_columns.size(); ++i) {
      if (table.second->GetColumnType(i) == FieldType::INT || table.second->GetColumnType(i) == FieldType::FLOAT) {
        StatsManager::GetInstance().Analyze(table.first, i);
      }
    }
  }
}

void SystemManager::Flush() {
  for (auto &table : tables_) {
    table.second->StoreMeta();
  }
  BufferManager::GetInstance().FlushAll();
}

void SystemManager::Recover() {
  std::cerr << "< ---------- SystemManager::Recover ------------ >\n";
  // TIPS: Recover算法
  LSN checkpoint_lsn = LoadMasterRecord();
  // Analyse过程
  log_manager_.Analyse(checkpoint_lsn);
  // Redo过程
  log_manager_.Redo();
  // Undo过程
  log_manager_.Undo();
}

void SystemManager::LoadLogManager() {
  std::cerr << "< ---------- SystemManager::LoadLogManager ----------- >\n";
  if (using_db_.empty()) {
    throw NoUsingDatabaseError();
  }
  std::cerr << "< ----- 6 ----- >\n";
  log_data_fd_ = disk_manager_.OpenFile(LOG_DATA);
  log_index_fd_ = disk_manager_.OpenFile(LOG_INDEX);
  LSN checkpoint_lsn = LoadMasterRecord();
  std::cerr << "< ----- 7 ----- >\n";
  std::cerr << "ckpt: " << checkpoint_lsn << "\n";
  if (checkpoint_lsn != 0) {
    // Load Stored Checkpoint Log
    Log *log = ReadLog(checkpoint_lsn);
    assert(log->GetLSN() == checkpoint_lsn);
    delete log;
  } else {
    log_manager_.Init();
  }
  Recover();
}

void SystemManager::StoreLogManager() {
  if (using_db_.empty()) {
    throw NoUsingDatabaseError();
  }
  log_manager_.Checkpoint();
}

void SystemManager::WriteLog(Log *log) {
  size_t log_len = log->GetLength();
  Byte *raw_data = new Byte[log_len];
  LogFactory::StoreLog(raw_data, log);
  disk_manager_.AppendRaw(log_data_fd_, raw_data, log_len);
  disk_manager_.FlushFile(log_data_fd_);
  size_t fsize = disk_manager_.FileSize(LOG_DATA);
  disk_manager_.AppendRaw(log_index_fd_, (Byte *)&fsize, sizeof(size_t));
  disk_manager_.FlushFile(log_index_fd_);
  delete[] raw_data;
}

Log *SystemManager::ReadLog(LSN lsn) {
  size_t lsn_max = disk_manager_.FileSize(LOG_INDEX) / sizeof(size_t);
  size_t lsn_idx = lsn - INIT_LSN;
  if (lsn_idx >= lsn_max) return nullptr;
  size_t lsn_offset = 0, lsn_size = 0;
  if (lsn_idx == 0) {
    lsn_offset = 0;
    lsn_size = disk_manager_.ReadIndex(log_index_fd_, lsn_idx);
  } else {
    lsn_offset = disk_manager_.ReadIndex(log_index_fd_, lsn_idx - 1);
    lsn_size = disk_manager_.ReadIndex(log_index_fd_, lsn_idx) - lsn_offset;
  }
  Byte *raw_data = new Byte[lsn_size];
  disk_manager_.ReadRaw(log_data_fd_, raw_data, lsn_size, lsn_offset);
  std::cerr << "< ----- 8 ----- >\n";
  Log *log = LogFactory::LoadLog(raw_data);
  std::cerr << "< ----- 9 ----- >\n";
  delete[] raw_data;
  return log;
}

void SystemManager::InitLog(const string &db_name) {
  // 切换目录
  std::string directory;
  if (!using_db_.empty()) {
    directory = "../" + db_name + "/";
  } else {
    directory = db_name + "/";
  }
  // 创建Log文件
  disk_manager_.CreateFile(directory + LOG_INDEX);
  disk_manager_.CreateFile(directory + LOG_DATA);
  // 初始化MASTER RECORD
  disk_manager_.CreateFile(directory + MASTER_RECORD);
  int master_fd = disk_manager_.OpenFile(directory + MASTER_RECORD);
  const LSN init_lsn = 0;
  disk_manager_.WriteRaw(master_fd, (Byte *)&init_lsn, sizeof(LSN));
  disk_manager_.CloseFile(master_fd);
}

std::string SystemManager::GetTableByColumn(const std::string &col_name, const std::vector<std::string> &table_names) {
  bool found = false;
  std::string table_name_of_col;
  for (const auto &table_name : table_names) {
    auto column_names = GetTable(table_name)->GetColumnNames();
    if (std::find(column_names.begin(), column_names.end(), col_name) != column_names.end()) {
      if (found) {
        throw ColumnAmbiguousError(col_name);
      } else {
        table_name_of_col = table_name;
        found = true;
      }
    }
  }
  if (!found) {
    throw ColumnNotFoundError(col_name);
  }
  return table_name_of_col;
}

std::string SystemManager::GetTableByFd(int fd) { return fd2table_[fd]; }

void SystemManager::StoreMasterRecord() {
  if (using_db_.empty()) {
    throw NoUsingDatabaseError();
  }
  int master_fd = disk_manager_.OpenFile(MASTER_RECORD);
  LSN current_lsn = log_manager_.GetCurrent();
  disk_manager_.WriteRaw(master_fd, (Byte *)&current_lsn, sizeof(LSN));
  disk_manager_.CloseFile(master_fd);
}

LSN SystemManager::LoadMasterRecord() {
  if (using_db_.empty()) {
    throw NoUsingDatabaseError();
  }
  LSN checkpoint_lsn = 0;
  int master_fd = disk_manager_.OpenFile(MASTER_RECORD);
  disk_manager_.ReadRaw(master_fd, (Byte *)&checkpoint_lsn, sizeof(LSN));
  disk_manager_.CloseFile(master_fd);
  return checkpoint_lsn;
}

}  // namespace dbtrain
