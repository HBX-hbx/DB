#include "histogram.h"

#include <algorithm>
#include "defines.h"
#include <float.h>

namespace dbtrain {

Histogram::Histogram(int num_buckets) : num_buckets_(num_buckets) {
  total_ = 0;
  counts_ = new int[num_buckets];
  for (int i = 0; i < num_buckets; i++) {
    counts_[i] = 0;
  }
}

Histogram::~Histogram() { delete[] counts_; }

int Histogram::GetRecordNum() const { return total_; }

void Histogram::Init(const vector<double> &val_list) {
  // TODO: 根据val_list生成直方图
  // TIPS: 先判断数据上下界范围，之后按照同数量划分区间
  // TIPS: 按照划分区间统计各个桶内的数据量
  // TIPS: 同时记录总数据量
  // LAB 5 BEGIN
  min_value_ = DBL_MAX;
  max_value_ = DBL_MIN;
  for (auto &val: val_list) {
    min_value_ = std::min(min_value_, val);
    max_value_ = std::max(max_value_, val);
  }
  total_ = val_list.size();
  min_value_ -= EPSILON;
  max_value_ += EPSILON;
  width_ = (max_value_ - min_value_) / num_buckets_;
  for (auto &val: val_list) {
    int bucket_id = (val - min_value_) / width_;
    counts_[bucket_id]++;
  }
  // LAB 5 END
}

double Histogram::LowerBound(double lower) const {
  // TODO: 统计并返回大于等于lower的数据比例
  // TIPS: 注意超界判断
  // TIPS: 注意返回比例，不是总量
  // LAB 5 BEGIN
  if (lower < min_value_) return 1.0;
  if (lower > max_value_) return 0.0;

  int bucket_id = (lower - min_value_) / width_; // 落在哪个桶里
  double x = (lower - min_value_) / width_; // 横坐标
  double cnt = 0.0; // 统计数据总量

  cnt += counts_[bucket_id] * (bucket_id + 1 - x);
  for (int i = bucket_id + 1; i < num_buckets_; ++i) {
    cnt += counts_[i];
  }

  return cnt / total_;
  // LAB 5 END
}

double Histogram::UpperBound(double upper) const {
  // TODO: 统计并返回小于upper的数据比例
  // TIPS: 注意超界判断
  // TIPS: 注意返回比例，不是总量
  // LAB 5 BEGIN
  if (upper < min_value_) return 0.0;
  if (upper > max_value_) return 1.0;

  int bucket_id = (upper - min_value_) / width_; // 落在哪个桶里
  double x = (upper - min_value_) / width_; // 横坐标
  double cnt = 0.0; // 统计数据总量

  cnt += counts_[bucket_id] * (x - bucket_id);
  for (int i = 0; i < bucket_id; ++i) {
    cnt += counts_[i];
  }

  return cnt / total_;
  // LAB 5 END
}

double Histogram::RangeBound(double lower, double upper) const {
  // TODO: 统计并返回介于lower和upper之间的数据比例
  // TIPS: 注意超界判断
  // TIPS: 注意返回比例，不是总量
  // LAB 5 BEGIN
  return 1.0 - UpperBound(lower) - LowerBound(upper);
  // LAB 5 END
}

}  // namespace dbtrain
