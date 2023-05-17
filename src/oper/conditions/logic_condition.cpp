#include "logic_condition.h"
#include <algorithm>
#include "oper/conditions/algebra_condition.h"

namespace dbtrain {

AndCondition::AndCondition(const vector<Condition *> &conds) : conds_(conds) {}

AndCondition::~AndCondition() {
  for (const auto &cond : conds_)
    if (cond != nullptr) delete cond;
}

bool AndCondition::Fit(Record *record) const {
  for (const auto &cond : conds_)
    if (cond != nullptr)
      if (!cond->Fit(record)) return false;
  return true;
}

vector<int> AndCondition::GetColIdxs() const {
  vector<int> col_idxs{};
  vector<Condition *> conds = GetConditions();
  while (true) {
    // 先处理右边条件，由于左深树，一定为 AlgebraCondition
    AlgebraCondition *ri_cond = dynamic_cast<AlgebraCondition *>(conds[1]);
    col_idxs.push_back(ri_cond->GetIdx());
    // 再处理左边条件，可能为 AndCondition 继续递归
    AlgebraCondition *le_cond = dynamic_cast<AlgebraCondition *>(conds[0]);
    if (le_cond != nullptr) { // 说明达到递归叶子，break
      col_idxs.push_back(le_cond->GetIdx());
      break;
    }
    conds = dynamic_cast<AndCondition *>(conds[0])->GetConditions();
  }
  // 去重 + 排序
  std::sort(col_idxs.begin(), col_idxs.end());
  col_idxs.erase(std::unique(col_idxs.begin(), col_idxs.end()), col_idxs.end());
  return col_idxs;
}

void AndCondition::PushBack(Condition *cond) { conds_.push_back(cond); }

vector<Condition *> AndCondition::GetConditions() const { return conds_; }

void AndCondition::Display() const {
  for (int i = 0; i < conds_.size() - 1; ++i) {
    conds_[i]->Display();
    printf("&");
  }
  conds_[conds_.size() - 1]->Display();
}

OrCondition::OrCondition(const vector<Condition *> &conds) : conds_(conds) {}

OrCondition::~OrCondition() {
  for (const auto &cond : conds_)
    if (cond != nullptr) delete cond;
}

void OrCondition::PushBack(Condition *cond) { conds_.push_back(cond); }

bool OrCondition::Fit(Record *record) const {
  for (const auto &cond : conds_)
    if (cond != nullptr)
      if (cond->Fit(record)) return true;
  return false;
}

vector<int> OrCondition::GetColIdxs() const {
  // 该函数不会被调用
  assert(false);
}

void OrCondition::Display() const {
  for (int i = 0; i < conds_.size() - 1; ++i) {
    conds_[i]->Display();
    printf("|");
  }
  conds_[conds_.size() - 1]->Display();
}

NotCondition::NotCondition(Condition *cond) : cond_(cond) {}

NotCondition::~NotCondition() {
  if (cond_ != nullptr) delete cond_;
}

bool NotCondition::Fit(Record *record) const {
  if (cond_ == nullptr) return false;
  return !cond_->Fit(record);
}

vector<int> NotCondition::GetColIdxs() const {
  // 该函数不会被调用
  assert(false);
}

void NotCondition::Display() const {
  printf("!");
  cond_->Display();
}

}  // namespace dbtrain
