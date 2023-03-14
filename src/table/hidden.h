#ifndef DBTRAIN_HIDDEN_H
#define DBTRAIN_HIDDEN_H

#include "../defines.h"
#include "table_meta.h"

namespace dbtrain {

// 隐藏列的配置，所有的隐藏列都以"-"为开头
const std::vector<Column> hidden_columns = {{FieldType::INT, 4, "-PAGE_ID"},
                                            {FieldType::INT, 4, "-SLOT_ID"},
                                            {FieldType::INT, 4, "-CREATE_XID"},
                                            {FieldType::INT, 4, "-DELETE_XID"}};

// 隐藏列的倒序，用于从结尾倒序访问
const int PAGE_ID_OFFSET = 4;
const int SLOT_ID_OFFSET = 3;
const int CREATE_XID_OFFSET = 2;
const int DELETE_XID_OFFSET = 1;

}  // namespace dbtrain

#endif
