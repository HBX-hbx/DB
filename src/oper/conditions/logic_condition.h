#ifndef DBTRAIN_LOGIC_CONDITION
#define DBTRAIN_LOGIC_CONDITION

#include "condition.h"

namespace dbtrain {

class AndCondition : public Condition {
 public:
  AndCondition(const vector<Condition *> &conds);
  ~AndCondition();
  bool Fit(Record *record) const override;
  void PushBack(Condition *cond);
  vector<int> GetColIdxs() const override;

  virtual void Display() const override;

  vector<Condition *> GetConditions() const;

 private:
  vector<Condition *> conds_;
};

class OrCondition : public Condition {
 public:
  OrCondition(const vector<Condition *> &conds);
  ~OrCondition();
  bool Fit(Record *record) const override;
  void PushBack(Condition *cond);
  vector<int> GetColIdxs() const override;

  virtual void Display() const override;

 private:
  vector<Condition *> conds_;
};

class NotCondition : public Condition {
 public:
  NotCondition(Condition *cond);
  ~NotCondition();
  bool Fit(Record *record) const override;
  vector<int> GetColIdxs() const override;

  virtual void Display() const override;

 private:
  Condition *cond_;
};

}  // namespace dbtrain

#endif
