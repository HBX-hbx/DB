#include "algebra_condition.h"

namespace dbtrain {

AlgebraCondition::~AlgebraCondition() { delete field_; }
int AlgebraCondition::GetIdx() const { return idx_; }
Field *AlgebraCondition::GetField() const { return field_; }

vector<int> AlgebraCondition::GetColIdxs() const { return {idx_}; }

EqualCondition::EqualCondition(int idx, Field *field) : AlgebraCondition(idx, field) {}

bool EqualCondition::Fit(Record *record) const { return record->GetField(idx_)->Equal(field_); }

void EqualCondition::Display() const { printf("\t%s", field_->ToString().c_str()); }

LessCondition::LessCondition(int idx, Field *field) : AlgebraCondition(idx, field) {}

bool LessCondition::Fit(Record *record) const { return record->GetField(idx_)->Less(field_); }

void LessCondition::Display() const { printf("<%s", field_->ToString().c_str()); }

GreaterCondition::GreaterCondition(int idx, Field *field) : AlgebraCondition(idx, field) {}

bool GreaterCondition::Fit(Record *record) const { return record->GetField(idx_)->Greater(field_); }

void GreaterCondition::Display() const { printf(">%s", field_->ToString().c_str()); }

}  // namespace dbtrain
