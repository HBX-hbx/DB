#include "optim.h"

#include <cfloat>
#include <queue>
#include <unordered_map>
#include <unordered_set>

#include "../exception/exceptions.h"
#include "../exception/table_exceptions.h"
#include "../oper/conditions/conditions.h"
#include "../oper/conditions/join_condition.h"
#include "../oper/nodes.h"
#include "oper/conditions/logic_condition.h"
#include "oper/join_node.h"
#include "stats_manager.h"
#include "../system/system_manager.h"
#include "../utils/graph.h"
#include "../utils/uf_set.h"

namespace dbtrain {

typedef std::pair<double, double> LUBound;

const string delimiter = ".";

Optimizer::Optimizer() { meta_ = &SystemManager::GetInstance(); }

Optimizer::~Optimizer() {}

std::any Optimizer::visit(Insert *insert) {
  RecordList record_list{};
  for (const auto &vals : insert->vals_list_) {
    Record *record = new Record();
    for (const auto &val : vals) {
      auto field = std::any_cast<Field *>(val->accept(this));
      record->PushBack(field);
    }
    record_list.push_back(record);
  }
  Table *table = meta_->GetTable(insert->table_name_);
  OperNode *plan = new InsertNode(table, record_list);
  return plan;
}

std::any Optimizer::visit(Delete *delete_) {
  string table_name = delete_->table_name_;
  // 初始化全表扫描结点
  OperNode *plan = new TableScanNode(meta_->GetTable(table_name));
  // 添加选择条件算子
  if (delete_->condition_ != nullptr) delete_->condition_->accept(this);

  auto it = table_filter_.find(table_name);
  if (it != table_filter_.end()) {
    FilterNode *node = new FilterNode(plan, table_filter_[table_name]);
    node->SetFilterTable(table_name);
    plan = node;
    table_filter_.erase(it);
  }
  // 返回删除结点
  plan = new DeleteNode(plan, meta_->GetTable(table_name));
  return plan;
}

std::any Optimizer::visit(Update *update) {
  string table_name = update->table_name_;
  // 初始化全表扫描结点
  OperNode *plan = new TableScanNode(meta_->GetTable(table_name));
  // 添加选择条件算子
  if (update->condition_ != nullptr) update->condition_->accept(this);
  auto it = table_filter_.find(table_name);
  if (it != table_filter_.end()) {
    FilterNode *node = new FilterNode(plan, table_filter_[table_name]);
    node->SetFilterTable(table_name);
    plan = node;
    table_filter_.erase(it);
  }
  // 添加SET条件
  vector<pair<int, Field *>> set_clauses{};
  for (const auto &set_clause : update->set_clauses_) {
    auto set_pair = std::any_cast<pair<string, Field *>>(set_clause->accept(this));
    auto col_idx = meta_->GetTable(table_name)->GetColumnIdx(set_pair.first);
    // 类型检测
    auto col_type = meta_->GetTable(table_name)->GetColumnType(col_idx);
    if (col_type != set_pair.second->GetType()) {
      throw InvalidUpdateTypeError(type2str[set_pair.second->GetType()], type2str[col_type]);
    }
    set_clauses.push_back({col_idx, set_pair.second});
  }
  // 返回更新结点
  plan = new UpdateNode(plan, meta_->GetTable(table_name), set_clauses);
  return plan;
}

std::any Optimizer::visit(Select *select) {
  auto uf_set = UFSet<string>(select->tables_);
  // 初始化全表扫描结点
  std::unordered_map<string, OperNode *> table_map{};
  for (const auto &table_name : select->tables_) {
    table_map[table_name] = new TableScanNode(meta_->GetTable(table_name));
    table_shift_[table_name] = 0;
  }
  // 添加选择条件算子
  if (select->condition_ != nullptr) select->condition_->accept(this);
  for (const auto &table_name : select->tables_) {
    auto it = table_filter_.find(table_name);
    if (it != table_filter_.end()) {
      FilterNode *node = new FilterNode(table_map[table_name], table_filter_[table_name]);
      node->SetFilterTable(table_name);
      table_map[table_name] = node;
      table_filter_.erase(it);
    }
  }

  // TODO: 基于基数优化JOIN的执行顺序
  // TIPS: 基础功能不需要考虑包含or情景的计划优化
  // TIPS: 首先需要完成直方图结构的相关函数
  // TIPS: 利用OperNode的Cost函数可以估计FilterNode和TableScanNode的基数
  // TIPS: 考虑到Join算子基数估计较为困难，基础功能仅做如下要求：
  // TIPS: (1) 估计join操作前table_map各个算子的基数
  // TIPS: (2) 按照join构建表的连接图（无向图位于utils/graph）
  // TIPS: (3) 按照算子基数，从基数最低的节点开始进行宽度优先搜索BFS，按照结点添加顺序生成join顺序
  // TIPS: 可以新建vector保存join顺序并修改LAB 3生成执行计划的顺序
  // TIPS: 注意，文档中给出了一个简单的例子
  // LAB 5 BEGIN
  // (1) 估计join操作前table_map各个算子的基数
  std::map<string, double> costs;
  double min_cost = DBL_MAX;
  string min_table_name; // 开始 bfs 的 table_name
  for (auto &table_pair: table_map) {
    string table_name = table_pair.first;
    double cost = table_pair.second->Cost();
    costs[table_name] = cost;
    if (cost < min_cost) {
      min_cost = cost;
      min_table_name = table_name;
    }
  }
  // (2) 按照join构建表的连接图（无向图位于utils/graph）
  auto graph = new UndirectedGraph<std::pair<string, double>>;
  // 根据 table_filter 构建无向图，加边
  for (auto &join_pair: table_filter_) {
    string join_table_name = join_pair.first;
    size_t pos = join_table_name.find(delimiter);
    string lhs_table_name = join_table_name.substr(0, pos);
    string rhs_table_name = join_table_name.substr(pos + 1, join_table_name.size());

    graph->AddEdge({lhs_table_name, costs[lhs_table_name]}, {rhs_table_name, costs[rhs_table_name]});
  }
  // (3) 按照算子基数，从基数最低的节点开始进行宽度优先搜索BFS，按照结点添加顺序生成join顺序
  std::queue<string> q; // bfs 队列
  std::map<string, bool> visited; // 是否访问过该 table 节点
  vector<string> order; // 连接顺序，比如 t1.t2, t2.t3 ...
  vector<std::pair<string, double>> neighbors;

  q.push(min_table_name);
  visited[min_table_name] = true;
  while(!q.empty()) {
    string curr = q.front();
    q.pop();
    min_cost = DBL_MAX; // 对当前节点 curr，找到最小基数结点，并添加到已连接结点集

    bool exist_non_visited_neighbor = false; // 是否存在未访问过的邻居
    for (auto adj_table_pair: graph->Adjace({curr, costs[curr]})) {
      neighbors.push_back(adj_table_pair);
    }
    for (auto neighbor_pair: neighbors) {
      // 没访问过，则访问
      if (!visited[neighbor_pair.first]) {
        exist_non_visited_neighbor = true;
        double cost = neighbor_pair.second;
        if (cost < min_cost) {
          min_cost = cost;
          min_table_name = neighbor_pair.first;
        }
      }
    }
    // 删除选中的节点
    std::pair<string, double> ele = {min_table_name, costs[min_table_name]};
    neighbors.erase(std::remove(neighbors.begin(), neighbors.end(), ele), neighbors.end());
    if (exist_non_visited_neighbor) {
      visited[min_table_name] = true;
      q.push(min_table_name);
      // 在已连接的所有节点里，找能跟 table_filter 里匹配上的加进 order 中
      for (auto &visited_pair: visited) {
        // 已经连接的
        if (visited_pair.second) {
          string join_table_name = visited_pair.first + delimiter + min_table_name;
          // 可能是反序的，试着反过来看看
          if (table_filter_.find(join_table_name) == table_filter_.end()) {
            join_table_name = min_table_name + delimiter + visited_pair.first;
          }
          // 如果找到了，就加进去 order，并且 break
          if (table_filter_.find(join_table_name) != table_filter_.end()) {
            order.push_back(join_table_name);
            break;
          }
        }
      }
    }
  }
  // LAB 5 END

  // TODO: 添加连接算子
  // TIPS: 遍历 table_filter_ 中的每一个 join_condition
  // TIPS: 在 table_map 中新建 JoinNode
  // TIPS: 使用 uf_set 维护表的连接关系
  // TIPS: 需维护 table_shift_ 中的偏移量，以使投影算子可以正常工作
  // LAB 4 BEGIN
  for (auto &join_table_name: order) {
    JoinCondition *join_condition = (JoinCondition *)table_filter_[join_table_name];

    size_t pos = join_table_name.find(delimiter);
    string lhs_table_name = join_table_name.substr(0, pos);
    lhs_table_name = uf_set.Find(lhs_table_name);
    string rhs_table_name = join_table_name.substr(pos + 1, join_table_name.size());
    rhs_table_name = uf_set.Find(rhs_table_name);

    vector<string> lhs_all_tables = uf_set.FindAll(lhs_table_name);
    vector<string> rhs_all_tables = uf_set.FindAll(rhs_table_name);
    int shift_offset = 0;
    // 统计左边 table 家族中所有已连接的表的总列数，作为右边 table 家族增加的偏移量
    for (auto &lhs_table: lhs_all_tables) {
      shift_offset += meta_->GetTable(lhs_table)->GetColumnSize();
    }
    for (auto &rhs_table: rhs_all_tables) {
      table_shift_[rhs_table] += shift_offset;
    }
    table_map[lhs_table_name] = new JoinNode(
        table_map[lhs_table_name], 
        table_map[rhs_table_name], 
        join_condition
      );
    
    uf_set.Union(rhs_table_name, lhs_table_name);
  }
  // LAB 4 END

  string first_table = uf_set.Find(select->tables_[0]);

  // 检验所有的表都已经被JOIN
  for (const auto &table_name : select->tables_) {
    if (uf_set.Find(table_name) != first_table) throw TableNotJoinedError(table_name);
  }

  // 添加投影算子
  OperNode *plan = table_map[first_table];
  if (!select->cols_.empty()) {
    vector<int> proj_idxs{};
    for (const auto &col : select->cols_) {
      int offset = table_shift_[col->table_name_];
      proj_idxs.push_back(offset + meta_->GetTable(col->table_name_)->GetColumnIdx(col->col_name_));
    }
    plan = new ProjectNode(plan, proj_idxs);
  }
  // 返回选择算子
  plan = new SelectNode(plan);
  return plan;
}

std::any Optimizer::visit(LessConditionNode *cond) {
  // 小于选择条件
  string table_name = cond->col_->table_name_;
  string col_name = cond->col_->col_name_;
  int col_idx = meta_->GetTable(table_name)->GetColumnIdx(col_name);
  Field *field = std::any_cast<Field *>(cond->value_->accept(this));
  Condition *condition = new LessCondition(col_idx, field);
  if (table_filter_.find(table_name) == table_filter_.end()) {
    table_filter_[table_name] = condition;
    condition = nullptr;
  }
  std::pair<string, Condition *> cpair{table_name, condition};
  return cpair;
}

std::any Optimizer::visit(EqualConditionNode *cond) {
  // 等值选择条件
  string table_name = cond->col_->table_name_;
  string col_name = cond->col_->col_name_;
  int col_idx = meta_->GetTable(table_name)->GetColumnIdx(col_name);
  Field *field = std::any_cast<Field *>(cond->value_->accept(this));
  Condition *condition = new EqualCondition(col_idx, field);
  if (table_filter_.find(table_name) == table_filter_.end()) {
    table_filter_[table_name] = condition;
    condition = nullptr;
  }
  std::pair<string, Condition *> cpair{table_name, condition};
  return cpair;
}

std::any Optimizer::visit(GreaterConditionNode *cond) {
  // 大于选择条件
  string table_name = cond->col_->table_name_;
  string col_name = cond->col_->col_name_;
  int col_idx = meta_->GetTable(table_name)->GetColumnIdx(col_name);
  Field *field = std::any_cast<Field *>(cond->value_->accept(this));
  Condition *condition = new GreaterCondition(col_idx, field);
  if (table_filter_.find(table_name) == table_filter_.end()) {
    table_filter_[table_name] = condition;
    condition = nullptr;
  }
  std::pair<string, Condition *> cpair{table_name, condition};
  return cpair;
}

std::any Optimizer::visit(JoinConditionNode *cond) {
  // TODO: （高级功能）连接运算左右算子选择
  // TIPS: 注意，部分连接运算与左右算子顺序相关，需要调整正确的执行数据
  // TIPS: 注意，在基础功能测例中不检查，但是建议使用了顺序相关JOIN算法的同学正确调整JOIN顺序
  // LAB 5 BEGIN
  // LAB 5 END

  // TODO: 解析连接条件
  // TIPS: 将 JoinCondition 添加到 table_filter_ 中
  // TIPS: 可以将两个表名组合成一个新的表名，以 delimiter 变量为分隔符，作为 table_filter_ 的 key
  // LAB 4 BEGIN
  string table_name = cond->lhs_->table_name_ + delimiter + cond->rhs_->table_name_;
  int idx_left = meta_->GetTable(cond->lhs_->table_name_)->GetColumnIdx(cond->lhs_->col_name_);
  int idx_right = meta_->GetTable(cond->rhs_->table_name_)->GetColumnIdx(cond->rhs_->col_name_);
  Condition *condition = new JoinCondition(idx_left, idx_right);
  if (table_filter_.find(table_name) == table_filter_.end()) {
    table_filter_[table_name] = condition;
    condition = nullptr;
  }
  std::pair<string, Condition *> cpair{table_name, condition};
  return cpair;
  // LAB 4 END
}

std::any Optimizer::visit(AndConditionNode *and_condition) {
  // TODO: 条件合取
  // TIPS: 调用 and_condition lhs_ 和 rhs_ 的 accept 函数进行访问
  // TIPS: parser 会将多个 AND 条件解析为左深树
  // TIPS: 将 Condition 记录在 table_filter_ 中
  // TIPS: 函数需要添加返回值，否则可能出现段错误
  // LAB 4 BEGIN
  auto res = and_condition->rhs_->accept(this);
  if (res.has_value()) {
    auto cpair = std::any_cast<std::pair<string, Condition *>>(res);
    string table_name = cpair.first;
    if (cpair.second != nullptr) {
      table_filter_[table_name] = new AndCondition({table_filter_[table_name], cpair.second});
    }
  }
  res = and_condition->lhs_->accept(this);
  if (res.has_value()) {
    auto cpair = std::any_cast<std::pair<string, Condition *>>(res);
    string table_name = cpair.first;
    if (cpair.second != nullptr) {
      table_filter_[table_name] = new AndCondition({table_filter_[table_name], cpair.second});
    }
  }
  std::any null;
  return null;
  // LAB 4 END
}

std::any Optimizer::visit(OrConditionNode *or_condition) {
  // TODO: 条件析取
  // TIPS: 与 AndCondition 流程相同
  // LAB 4 BEGIN
  auto res = or_condition->rhs_->accept(this);
  if (res.has_value()) {
    auto cpair = std::any_cast<std::pair<string, Condition *>>(res);
    string table_name = cpair.first;
    if (cpair.second != nullptr) {
      table_filter_[table_name] = new OrCondition({table_filter_[table_name], cpair.second});
    }
  }
  res = or_condition->lhs_->accept(this);
  if (res.has_value()) {
    auto cpair = std::any_cast<std::pair<string, Condition *>>(res);
    string table_name = cpair.first;
    if (cpair.second != nullptr) {
      table_filter_[table_name] = new OrCondition({table_filter_[table_name], cpair.second});
    }
  }
  std::any null;
  return null;
  // LAB 4 END
}

}  // namespace dbtrain
