#ifndef DBTRAIN_JOIN_CONDITION_H
#define DBTRAIN_JOIN_CONDITION_H

#include "condition.h"
#include "../../record/record.h"

namespace dbtrain {

class JoinCondition : public Condition {
 public:
  JoinCondition(int idx_left, int idx_right);
  ~JoinCondition() = default;

  bool Fit(Record *record) const override;
  bool Fit(Record *left, Record *right) const;
  vector<int> GetColIdxs() const override;

  void LeftShift(int delta);
  void RightShift(int delta);

  int GetLeftIdx() const;
  int GetRightIdx() const;

  virtual void Display() const override;

 private:
  int idx_left_;
  int idx_right_;
};

}  // namespace dbtrain

#endif
