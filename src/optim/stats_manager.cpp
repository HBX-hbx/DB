#include "stats_manager.h"

#include <set>

#include "oper/scan_node.h"
#include "record/fields.h"
#include "system/system_manager.h"
#include "utils/histogram.h"

namespace dbtrain {

StatsManager::~StatsManager() {
  for (auto histogram : stats_map_) {
    delete histogram.second;
  }
}

StatsManager &StatsManager::GetInstance() {
  static StatsManager stats_manager;
  return stats_manager;
}

string StatsManager::GetStatsName(string table_name, int col_idx) { return table_name + "." + std::to_string(col_idx); }

double GetRecordValue(Record *record, int idx) {
  Field *field = record->GetField(idx);
  if (field->GetType() == FieldType::INT) {
    IntField *int_field = dynamic_cast<IntField *>(field);
    assert(int_field != nullptr);
    return int_field->GetValue();
  } else if (field->GetType() == FieldType::FLOAT) {
    FloatField *double_field = dynamic_cast<FloatField *>(field);
    assert(double_field != nullptr);
    return double_field->GetValue();
  } else {
    assert(false);
  }
}

void StatsManager::Clear() { stats_map_.clear(); }

bool StatsManager::HasHistogram(string table_name, int col_idx) const {
  return stats_map_.find(GetStatsName(table_name, col_idx)) != stats_map_.end();
}

double StatsManager::LowerBound(string table_name, int col_idx, double lower) {
  string stats_name = GetStatsName(table_name, col_idx);
  if (stats_map_.find(stats_name) == stats_map_.end()) return 1.0;
  Histogram *histogram = stats_map_[stats_name];
  return histogram->LowerBound(lower);
}
double StatsManager::UpperBound(string table_name, int col_idx, double upper) {
  string stats_name = GetStatsName(table_name, col_idx);
  if (stats_map_.find(stats_name) == stats_map_.end()) return 1.0;
  Histogram *histogram = stats_map_[stats_name];
  return histogram->UpperBound(upper);
}
double StatsManager::RangeBound(string table_name, int col_idx, double lower, double upper) {
  string stats_name = GetStatsName(table_name, col_idx);
  if (stats_map_.find(stats_name) == stats_map_.end()) return 1.0;
  Histogram *histogram = stats_map_[stats_name];
  return histogram->RangeBound(lower, upper);
}

bool StatsManager::Analyze(string table_name, int col_idx) {
  // TODO: 分析单个表上单个列的直方图
  // TIPS: 注意，不要求实现分析结果的持久化存储
  // TIPS: 依据实现方式，可以选择跳过已经存在的直方图，或者选择更新直方图
  // TIPS: 基于表存储的数据初始化直方图，建议基础功能中采用默认构造参数
  // LAB 5 BEGIN
  string stats_name = GetStatsName(table_name, col_idx);
  // 找得到，直接返回 false
  if (stats_map_.find(stats_name) != stats_map_.end()) return false;

  Table *table = SystemManager::GetInstance().GetTable(table_name);
  TableScanNode *node = new TableScanNode(table);
  // 获取表的所有记录
  RecordList record_list{};
  vector<double> val_list{};
  auto next_list = node->Next();
  while (next_list.size() > 0) {
    record_list.insert(record_list.end(), next_list.begin(), next_list.end());
    next_list = node->Next();
  }
  // 遍历表的所有记录，获得所有值，push 进 val_list
  for (auto record: record_list) {
    val_list.push_back(GetRecordValue(record, col_idx));
  }

  Histogram *histogram = new Histogram();
  histogram->Init(val_list);
  stats_map_[stats_name] = histogram;

  return true; // 找不到，返回 true
  // LAB 5 END
}

double LinearCounting(const vector<double> &vals) {
  if (vals.size() == 0) return 1;
  const size_t COUNT_SIZE = 10;
  // TODO: （高级功能）利用LinearCounting或HyperLogLog计算Distinct Value
  // TIPS: 注意，仅在Join基数估计中使用，基础功能中不需要考虑
  // TIPS: 可以使用C++自带的Hash函数进行散列，散列后转为整型进行分桶
  // TIPS: Linear Counting使用的m可自行调整，考虑到准确性和空间开销设为略小于vals.size()的近似值
  // TIPS: 数据量较低时(<COUNT_SIZE)，可以直接统计不同元素数量
  // LAB 5 BEGIN
  // LAB 5 END
}

int StatsManager::GetRecordNum(string table_name, int col_idx) {
  string stats_name = GetStatsName(table_name, col_idx);
  if (stats_map_.find(stats_name) == stats_map_.end()) return 0;
  Histogram *histogram = stats_map_[stats_name];
  return histogram->GetRecordNum();
}

double StatsManager::GetRPV(string table_name, int col_idx) {
  string stats_name = GetStatsName(table_name, col_idx);
  if (stats_rpv_.find(stats_name) != stats_rpv_.end()) return stats_rpv_[stats_name];
  // 均匀分布简化假设下计算RPV
  // 获取数据
  Table *table = SystemManager::GetInstance().GetTable(table_name);
  TableScanNode *node = new TableScanNode(table);
  auto record_list = node->Next();
  vector<double> val_list{};
  while (record_list.size() > 0) {
    for (const auto &record : record_list) {
      auto val = GetRecordValue(record, col_idx);
      val_list.push_back(val);
    }
    for (const auto &record : record_list) delete record;
    record_list = node->Next();
  }
  delete node;
  double rpv = LinearCounting(val_list);
  stats_rpv_[stats_name] = rpv;
  return stats_rpv_[stats_name];
}

}  // namespace dbtrain
