#include "basic_node.h"

#include <cfloat>
#include <cmath>

#include "conditions/conditions.h"
#include "oper/conditions/algebra_condition.h"
#include "oper/conditions/logic_condition.h"
#include "oper/db_node.h"
#include "optim/stats_manager.h"
#include "record/fields.h"

namespace dbtrain {

typedef std::pair<double, double> RangePair;

ProjectNode::ProjectNode(OperNode *child, const vector<int> &idx_vec) : OperNode({child}) { proj_idxs = idx_vec; }

RecordList ProjectNode::Next() {
  OperNode *child = childs_[0];
  RecordList inlist = child->Next();
  RecordList outlist{};
  // 投影运算
  for (const auto &record : inlist) {
    Record *proj_record = new Record();
    for (const auto &idx : proj_idxs) proj_record->PushBack(record->GetField(idx)->Copy());
    outlist.push_back(proj_record);
  }
  // 删除无用指针
  for (const auto record : inlist) {
    delete record;
  }
  return outlist;
}

double ProjectNode::Cost() {
  OperNode *child = childs_[0];
  return child->Cost();
}

void ProjectNode::Display(int depth) const {
  for (int i = 0; i < depth; ++i) printf("\t");
  printf("Project Node: ");
  for (const auto& idx : proj_idxs) printf("%d ", idx);
  printf("\n");
  for (const auto &child : childs_) child->Display(depth + 1);
}

FilterNode::FilterNode(OperNode *child, Condition *cond) : OperNode({child}), cond_(cond) {}

FilterNode::~FilterNode() { delete cond_; }

RecordList FilterNode::Next() {
  OperNode *child = childs_[0];
  RecordList inlist{};
  RecordList outlist{};
  while (outlist.empty()) {
    inlist = child->Next();
    if (inlist.empty()) break;
    for (const auto &record : inlist) {
      if (cond_->Fit(record))
        outlist.push_back(record);
      else
        delete record;
    }
  }
  return outlist;
}

double GetFieldValue(Field *field) {
  if (field->GetType() == FieldType::INT) {
    return dynamic_cast<IntField *>(field)->GetValue();
  } else if (field->GetType() == FieldType::FLOAT) {
    return dynamic_cast<FloatField *>(field)->GetValue();
  } else {
    return 0;
  }
}

int UpdateBound(Condition *cond, double &lower, double &upper) {
  // 自动判断一个AlgebraCondition并更新上下界，仅限于数值型
  // 返回约束的列号
  EqualCondition *equal_cond = dynamic_cast<EqualCondition *>(cond);
  if (equal_cond != nullptr) {
    double bound = GetFieldValue(equal_cond->GetField());
    if (bound > lower) lower = bound;
    if (equal_cond->GetField()->GetType() == FieldType::INT) bound = std::ceil(bound + EPSILON);
    if (bound + EPSILON < upper) upper = bound + EPSILON;
    return equal_cond->GetIdx();
  }
  GreaterCondition *great_cond = dynamic_cast<GreaterCondition *>(cond);
  if (great_cond != nullptr) {
    double bound = GetFieldValue(great_cond->GetField());
    if (bound > lower) lower = bound;
    return great_cond->GetIdx();
  }
  LessCondition *less_cond = dynamic_cast<LessCondition *>(cond);
  if (less_cond != nullptr) {
    double bound = GetFieldValue(less_cond->GetField());
    if (bound + EPSILON < upper) upper = bound + EPSILON;
    return less_cond->GetIdx();
  }
  assert(false);
}

double FilterNode::Cost() {
  // TIPS: 基础功能不需要考虑Or条件
  OrCondition *or_cond = dynamic_cast<OrCondition *>(cond_);
  if (or_cond != nullptr) return childs_[0]->Cost();
  // TIPS: 本次实验中仅存在3种情况:(1)无条件;(2)单过滤条件;(3)And的多过滤条件。均仅需要考虑数值型
  // TIPS: 注意处理AndCondition的方式需要和LAB 3合成AndCondition的方式匹配
  // TIPS: 首先，完成无过滤条件情景的Cost估计，直接返回子节点Cost即可
  // TIPS: 之后，考虑单个过滤条件。可以利用UpdateBound更新过滤范围
  // TIPS: 最后，考虑And多条件过滤情景，需要考虑相同列的过滤条件合并
  // TIPS: 此外，多个属性同时存在过滤条件时简化假设各个属性互相独立
  // TIPS: 利用StatsManager的相关函数可以估计过滤条件所占比例
  // LAB 5 BEGIN
  // 1.无过滤条件
  if (cond_ == nullptr) return childs_[0]->Cost();

  // 2.单过滤条件，无 and
  AndCondition* and_condition = dynamic_cast<AndCondition*>(cond_);
  if (and_condition == nullptr) {
    double lower = DBL_MIN, upper = DBL_MAX;
    // 获得约束列 id，以及上下界
    int col_idx = UpdateBound(cond_, lower, upper);
    // 获得数据比例
    double proportion = StatsManager::GetInstance().RangeBound(tname_, col_idx, lower, upper);
    // 返回基数
    return proportion * childs_[0]->Cost();
  }

  // 3.多过滤条件，有 and
  std::map<int, std::pair<double, double>> col_idx_to_bounds; // 记录 col_idx -> {lower, upper}
  vector<Condition *> conds = and_condition->GetConditions();
  // 递归处理左深树
  while (true) {
    // 先处理右边条件，由于左深树，一定为 AlgebraCondition
    AlgebraCondition *ri_cond = dynamic_cast<AlgebraCondition *>(conds[1]);
    int col_idx = ri_cond->GetIdx();
    // 找不到，则新建一个映射
    if (col_idx_to_bounds.find(col_idx) == col_idx_to_bounds.end()) {
      col_idx_to_bounds[col_idx] = {
        DBL_MIN, 
        DBL_MAX
      };
    }
    UpdateBound(ri_cond, col_idx_to_bounds[col_idx].first, col_idx_to_bounds[col_idx].second);

    // 再处理左边条件，可能为 AndCondition 继续递归
    AlgebraCondition *le_cond = dynamic_cast<AlgebraCondition *>(conds[0]);
    if (le_cond != nullptr) { // 说明达到递归叶子，break
      // 处理与右边条件一致
      int col_idx = le_cond->GetIdx();
      // 找不到，则新建一个映射
      if (col_idx_to_bounds.find(col_idx) == col_idx_to_bounds.end()) {
        col_idx_to_bounds[col_idx] = {
          DBL_MIN, 
          DBL_MAX
        };
      }
      UpdateBound(le_cond, col_idx_to_bounds[col_idx].first, col_idx_to_bounds[col_idx].second);
      break;
    }
    conds = dynamic_cast<AndCondition *>(conds[0])->GetConditions();
  }
  // 统计总基数
  double proportion = 1.0;
  for (auto &col_idx_to_bound: col_idx_to_bounds) {
    int col_idx = col_idx_to_bound.first;
    int lower = col_idx_to_bound.second.first;
    int upper = col_idx_to_bound.second.second;
    proportion *= StatsManager::GetInstance().RangeBound(tname_, col_idx, lower, upper);
  }
  return proportion * childs_[0]->Cost();
  // LAB 5 END
}

void FilterNode::SetFilterTable(string tname) { tname_ = tname; }

void FilterNode::Display(int depth) const {
  for (int i = 0; i < depth; ++i) printf("\t");
  printf("Filter Node:");
  // cond_->Display();
  printf("\n");
  for (const auto &child : childs_) child->Display(depth + 1);
}

}  // namespace dbtrain
