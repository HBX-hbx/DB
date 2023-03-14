#ifndef DBTRAIN_SYSTEMMANAGER_H
#define DBTRAIN_SYSTEMMANAGER_H

#include <string>
#include <unordered_map>

#include "../log/log_manager.h"
#include "../oper/conditions/conditions.h"
#include "../result/result.h"
#include "../storage/disk_manager.h"
#include "../table/table.h"

namespace dbtrain {

class SystemManager {
 public:
  SystemManager(const SystemManager &) = delete;
  void operator=(const SystemManager &) = delete;
  static SystemManager &GetInstance();

  Result ShowDatabases();
  Result CreateDatabase(const std::string &db_name);
  Result UseDatabase(const std::string &db_name);
  Result DropDatabase(const std::string &db_name, bool if_exists);
  void CloseDatabase();
  void CloseDatabase(const std::string &db_name);

  Result CreateTable(const std::string &table_name, const std::vector<Column> &columns);
  Result DropTable(const std::string &table_name);
  Result ShowTables();
  Result DescTable(const std::string &table_name);

  void LoadLogManager();
  void StoreLogManager();
  void WriteLog(Log *log);
  Log *ReadLog(LSN lsn);
  void Crash();
  void Flush();
  void Recover();
  void StoreMasterRecord();
  void UsingTest();
  void Analyze();

 public:
  Table *GetTable(const std::string &table_name);
  std::string GetTableByColumn(const std::string &col_name, const std::vector<std::string> &table_names);
  std::string GetTableByFd(int fd);

 private:
  SystemManager();
  DiskManager &disk_manager_;
  LogManager &log_manager_;
  std::string using_db_;
  std::vector<std::string> db_names_;
  std::unordered_map<std::string, Table *> tables_;
  std::unordered_map<std::string, int> table2datafd_;
  std::unordered_map<std::string, int> table2metafd_;
  std::unordered_map<int, std::string> fd2table_;
  int log_index_fd_;
  int log_data_fd_;

  void InitLog(const std::string &db_name);
  LSN LoadMasterRecord();
};

}  // namespace dbtrain

#endif  // DBTRAIN_SYSTEMMANAGER_H
