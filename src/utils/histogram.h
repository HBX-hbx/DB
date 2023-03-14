#ifndef DBTRAIN_HISTGRAM_H
#define DBTRAIN_HISTGRAM_H

#include "../defines.h"

namespace dbtrain {

class Histogram {
 public:
  Histogram(int num_buckets = 10);
  ~Histogram();

  void Init(const vector<double> &val_list);
  int GetRecordNum() const;
  // 除最后一个区间为左闭右闭外，其余区间均为左闭右开
  double LowerBound(double lower) const;
  double UpperBound(double upper) const;
  double RangeBound(double lower, double upper) const;

 private:
  // 直方图桶的个数
  int num_buckets_;
  // 数据总量
  int total_;
  // 每个桶的数据量
  int *counts_;
  // 数据上下界
  double min_value_;
  double max_value_;
  // 桶的宽度
  double width_;
};

}  // namespace dbtrain

#endif
