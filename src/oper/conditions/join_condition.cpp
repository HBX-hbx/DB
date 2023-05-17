#include "join_condition.h"

namespace dbtrain {

JoinCondition::JoinCondition(int idx_left, int idx_right) : idx_left_(idx_left), idx_right_(idx_right) {}

bool JoinCondition::Fit(Record *record) const {
  // 该函数不会被调用
  assert(false);
}

bool JoinCondition::Fit(Record *left, Record *right) const {
  return left->GetField(idx_left_)->Equal(right->GetField(idx_right_));
}

vector<int> JoinCondition::GetColIdxs() const {
  // 该函数不会被调用
  return {idx_left_, idx_right_};
}

void JoinCondition::LeftShift(int idx) { idx_left_ += idx; }

void JoinCondition::RightShift(int idx) { idx_right_ += idx; }

int JoinCondition::GetLeftIdx() const { return idx_left_; }
int JoinCondition::GetRightIdx() const { return idx_right_; }

void JoinCondition::Display() const { printf("JOIN"); }

}  // namespace dbtrain
