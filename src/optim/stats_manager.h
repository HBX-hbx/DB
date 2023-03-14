#ifndef DBTRAIN_STATS_MANAGER_H
#define DBTRAIN_STATS_MANAGER_H

#include <map>
#include <set>

#include "../defines.h"
#include "../record/record.h"
#include "../utils/histogram.h"

namespace dbtrain {

class StatsManager {
 public:
  StatsManager(const StatsManager &) = delete;
  void operator=(const StatsManager &) = delete;
  static StatsManager &GetInstance();

  ~StatsManager();
  // 切换数据库时清空
  void Clear();

  static string GetStatsName(string table_name, int col_idx);
  // 分析单个表的统计信息，构建直方图
  bool Analyze(string table_name, int col_idx);

  // 范围过滤比例，估计对应表和列上给定范围内的数据比例
  // 下界条件，大于等于lower的比例
  double LowerBound(string table_name, int col_idx, double lower);
  // 上界条件，小于upper的比例
  double UpperBound(string table_name, int col_idx, double upper);
  // 范围条件，位于[lower, upper)的数据比例
  double RangeBound(string table_name, int col_idx, double lower, double upper);
  // 是否进行过直方图统计
  bool HasHistogram(string table_name, int col_idx) const;
  // 记录的数量
  int GetRecordNum(string table_name, int col_idx);

  // 估计单表的Record Per Value(RPV)，用于Join基数估计，基础功能不需要考虑
  double GetRPV(string table_name, int col_idx);

 private:
  // 表名+列序号 到 对应直方图的映射
  StatsManager() = default;
  std::map<string, Histogram *> stats_map_;
  std::map<string, double> stats_rpv_;
};

}  // namespace dbtrain

#endif
