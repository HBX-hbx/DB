#ifndef ALGEBRA_CONDITION_H
#define ALGEBRA_CONDITION_H

#include "condition.h"

namespace dbtrain {

class AlgebraCondition : public Condition {
 public:
  AlgebraCondition(int idx, Field *field) : idx_(idx), field_(field){};
  ~AlgebraCondition();

  int GetIdx() const;
  vector<int> GetColIdxs() const override;
  Field *GetField() const;

 protected:
  int idx_;
  Field *field_;
};

class EqualCondition : public AlgebraCondition {
 public:
  EqualCondition(int idx, Field *field);
  ~EqualCondition() = default;
  bool Fit(Record *record) const override;
  virtual void Display() const override;
};

class LessCondition : public AlgebraCondition {
 public:
  LessCondition(int idx, Field *field);
  ~LessCondition() = default;
  bool Fit(Record *record) const override;
  virtual void Display() const override;
};

class GreaterCondition : public AlgebraCondition {
 public:
  GreaterCondition(int idx, Field *field);
  ~GreaterCondition() = default;
  bool Fit(Record *record) const override;
  virtual void Display() const override;
};

}  // namespace dbtrain

#endif
