#include "visitor.h"

#include <any>
#include <thread>
#include <unordered_map>

#include "../exception/exceptions.h"
#include "../exec/exec.h"
#include "../oper/conditions/conditions.h"
#include "../oper/tx_node.h"
#include "../optim/optim.h"
#include "../parser/preprocessor.h"
#include "../record/fields.h"
#include "../record/record.h"
#include "../result/print_buffer.h"
#include "../result/result.h"
#include "../system/system_manager.h"
#include "ast.h"

namespace dbtrain {

namespace ast {

std::unordered_map<ast::SvType, FieldType> type_map = {{ast::SvType::SV_TYPE_INT, FieldType::INT},
                                                       {ast::SvType::SV_TYPE_FLOAT, FieldType::FLOAT},
                                                       {ast::SvType::SV_TYPE_STRING, FieldType::STRING}};

Visitor::~Visitor() {
  for (const auto &pair : declared_sqls_) {
    if (pair.second) delete pair.second;
  }
}

std::any Visitor::visit(SQL *) { return nullptr; }

std::any Visitor::visit(ShowDatabases *) { return SystemManager::GetInstance().ShowDatabases(); }

std::any Visitor::visit(CreateDatabase *create_database) {
  return SystemManager::GetInstance().CreateDatabase(create_database->db_name_);
}

std::any Visitor::visit(UseDatabase *use_database) {
  return SystemManager::GetInstance().UseDatabase(use_database->db_name_);
}

std::any Visitor::visit(DropDatabase *drop_database) {
  return SystemManager::GetInstance().DropDatabase(drop_database->db_name_, drop_database->if_exists_);
}

std::any Visitor::visit(CreateTable *create_table) {
  std::vector<Column> cols;
  for (const auto &field : create_table->fields_) {
    for (const auto &col_exist : cols) {
      if (col_exist.name_ == field->col_name_) {
        throw DuplicateColumnError(field->col_name_);
      }
    }
    cols.push_back(Column{type_map[field->type_len_->type_], field->type_len_->len_, field->col_name_});
  }
  return SystemManager::GetInstance().CreateTable(create_table->table_name_, cols);
}

std::any Visitor::visit(ShowTables *) { return SystemManager::GetInstance().ShowTables(); }

std::any Visitor::visit(DescTable *desc_table) {
  return SystemManager::GetInstance().DescTable(desc_table->table_name_);
}

std::any Visitor::visit(DropTable *drop_table) {
  return SystemManager::GetInstance().DropTable(drop_table->table_name_);
}

std::any Visitor::visit(Insert *insert) {
  // 优化器
  Optimizer *optimizer = new Optimizer();
  auto plan = std::any_cast<OperNode *>(optimizer->visit(insert));
  delete optimizer;

  // 执行器
  auto res = Execute(plan);

  // 定义模式判断
  if (DeclareMode()) return Result({"SUCCESS"});

  return Result(std::vector<std::string>{"SUCCESS"});
}

std::any Visitor::visit(Delete *delete_) {
  // 预处理
  Preprocessor *preprocessor = new Preprocessor({delete_->table_name_});
  preprocessor->visit(delete_);
  delete preprocessor;

  // 优化器
  Optimizer *optimizer = new Optimizer();
  auto plan = std::any_cast<OperNode *>(optimizer->visit(delete_));
  delete optimizer;

  // 执行器
  auto res = Execute(plan);

  // 定义模式判断
  if (DeclareMode()) return Result({"SUCCESS"});

  return Result(std::vector<std::string>{"SUCCESS"});
}

std::any Visitor::visit(Update *update) {
  // 预处理
  Preprocessor *preprocessor = new Preprocessor({update->table_name_});
  preprocessor->visit(update);
  delete preprocessor;

  // 优化器
  Optimizer *optimizer = new Optimizer();
  auto plan = std::any_cast<OperNode *>(optimizer->visit(update));
  delete optimizer;

  // 执行器
  auto res = Execute(plan);

  // 定义模式判断
  if (DeclareMode()) return Result({"SUCCESS"});

  return Result(std::vector<std::string>{"SUCCESS"});
}

std::any Visitor::visit(SetClause *set_clause) {
  Field *field = std::any_cast<Field *>(set_clause->val_->accept(this));
  std::pair<string, Field *> set_pair(set_clause->col_name_, field);
  return set_pair;
}

std::any Visitor::visit(Select *select) {
  Preprocessor *preprocessor = new Preprocessor(select->tables_);
  preprocessor->visit(select);
  delete preprocessor;

  // 优化器
  Optimizer *optimizer = new Optimizer();
  auto plan = std::any_cast<OperNode *>(optimizer->visit(select));
  delete optimizer;

  // 执行器
  auto res = Execute(plan);

  // 定义模式判断
  if (DeclareMode()) return Result({"SUCCESS"});

  // 输出Header
  std::vector<string> header_vec{};
  for (const auto &col : select->cols_) {
    header_vec.push_back(col->col_name_);
  }

  Result result(header_vec);
  result.InsertRecords(res);
  return result;
}

std::any Visitor::visit(Explain *explain) {
  Select *select = dynamic_cast<Select *>(explain->sql_.get());
  assert(!DeclareMode());
  if (select == nullptr) throw UnknownError();

  Preprocessor *preprocessor = new Preprocessor(select->tables_);
  preprocessor->visit(select);
  delete preprocessor;

  // 优化器生成执行计划树
  Optimizer *optimizer = new Optimizer();
  auto plan = std::any_cast<OperNode *>(optimizer->visit(select));
  delete optimizer;

  // 调整输出，输出执行计划树
  plan->Display(0);
  delete plan;

  return Result({"EXPLAIN"});
}

std::any Visitor::visit(Analyze *analyze) {
  SystemManager::GetInstance().Analyze();
  return Result({"ANALYZE"});
}

std::any Visitor::visit(Declare *declare) {
  assert(executor_ == nullptr);
  assert(!DeclareMode());
  declaring_ = declare->declaring_;
  executor_ = new Executor();
  return Result({"DECLARE BEGIN"});
}

std::any Visitor::visit(EndDeclare *end_declare) {
  assert(executor_ != nullptr);
  assert(DeclareMode());
  assert(end_declare->declaring_ == declaring_);
  declared_sqls_[declaring_] = executor_;
  executor_ = nullptr;
  declaring_ = "";
  return Result({"DECLARE END"});
}

void ExecuteDeclared(string declare, Executor *exec) {
  // 按照Declare的名字添加输出缓冲
  while (!exec->Finished()) {
    auto record_list = exec->RunNext();
    PrintBuffer::GetInstance().PrintRecord(declare, record_list);
  }
  delete exec;
}

std::any Visitor::visit(Run *run) {
  assert(executor_ == nullptr);
  std::vector<std::pair<string, Executor *>> exec_vec{};
  for (const auto &declare : run->declares_) {
    exec_vec.push_back({declare, declared_sqls_[declare]});
    declared_sqls_.erase(declare);
  }
  // 单独开线程执行各个Executor
  // 完成后删除对应Declare并释放Executor
  PrintBuffer &print_buffer = PrintBuffer::GetInstance();
  print_buffer.InitBuffer(run->declares_);
  std::vector<std::thread *> thread_list{};
  for (const auto &exec_pair : exec_vec) {
    std::thread *exec_thread = new std::thread(ExecuteDeclared, exec_pair.first, exec_pair.second);
    thread_list.push_back(exec_thread);
  }
  for (const auto &exec_thread : thread_list) {
    exec_thread->join();
    delete exec_thread;
  }
  for (const auto &declare : run->declares_) print_buffer.PrintOut(declare);
  return Result({"MULTITHREAD RUN"});
}

std::any Visitor::visit(Signal *signal) {
  assert(DeclareMode());
  auto sname = signal->signal_name_;
  OperNode *signal_node = new SignalNode(sname);
  Execute(signal_node);
  return Result({"SUCCESS"});
}

std::any Visitor::visit(Wait *wait) {
  assert(DeclareMode());
  auto sname = wait->signal_name_;
  OperNode *wait_node = new WaitNode(sname);
  Execute(wait_node);
  return Result({"SUCCESS"});
}

std::any Visitor::visit(Begin *) {
  if (!DeclareMode()) {
    assert(executor_ == nullptr);
    executor_ = new Executor();
  }
  OperNode *begin_node = new BeginNode();
  Execute(begin_node);
  if (DeclareMode()) {
    return Result({"SUCCESS"});
  }
  return Result({"SUCCESS"});
}

std::any Visitor::visit(Commit *) {
  if (!DeclareMode()) {
    assert(executor_ != nullptr);
  }
  OperNode *commit_node = new CommitNode();
  Execute(commit_node);
  if (DeclareMode()) {
    return Result({"SUCCESS"});
  } else {
    delete executor_;
    executor_ = nullptr;
  }
  return Result({"SUCCESS"});
}

std::any Visitor::visit(Abort *) {
  if (!DeclareMode()) {
    assert(executor_ != nullptr);
  }
  OperNode *abort_node = new AbortNode();
  Execute(abort_node);
  if (DeclareMode()) {
    return Result({"SUCCESS"});
  } else {
    delete executor_;
    executor_ = nullptr;
  }
  return Result({"SUCCESS"});
}

std::any Visitor::visit(Crash *) {
  if (executor_ != nullptr) delete executor_;
  executor_ = nullptr;
  SystemManager::GetInstance().Crash();
  return Result({"CRASH"});
}

std::any Visitor::visit(Checkpoint *) {
  LogManager::GetInstance().Checkpoint();
  return Result({"SUCCESS"});
}

std::any Visitor::visit(Flush *) {
  SystemManager::GetInstance().Flush();
  return Result({"FLUSH"});
}

std::any Visitor::visit(Value *) {
  assert(false);
  return nullptr;
}

std::any Visitor::visit(IntValue *int_value) {
  Field *field = new IntField(int_value->val_);
  return field;
}

std::any Visitor::visit(FloatValue *float_value) {
  Field *field = new FloatField(float_value->val_);
  return field;
}

std::any Visitor::visit(StringValue *str_value) {
  Field *field = new StrField(str_value->val_.c_str(), str_value->val_.size());
  return field;
}

std::any Visitor::visit(ConditionNode *) { assert(false); }

std::any Visitor::visit(AlgebraConditionNode *) { assert(false); }

std::any Visitor::visit(LessConditionNode *less_cond) { return nullptr; }

std::any Visitor::visit(EqualConditionNode *equal_cond) { return nullptr; }

std::any Visitor::visit(GreaterConditionNode *greater_cond) { return nullptr; }

std::any Visitor::visit(JoinConditionNode *cond) { return nullptr; }

std::any Visitor::visit(AndConditionNode *and_condition) { return nullptr; }

std::any Visitor::visit(OrConditionNode *or_condition) { return nullptr; }

RecordList Visitor::Execute(OperNode *plan) {
  if (DeclareMode()) {
    assert(executor_ != nullptr);
    executor_->AppendPlan(plan);
    return {};
  }
  if (executor_ != nullptr) {
    executor_->AppendPlan(plan);
    return executor_->RunNext();
  } else {
    Executor *exec = new Executor(plan);
    // 单条语句视为一个事务
    exec->Begin();
    auto res = exec->RunNext();
    exec->Commit();
    delete exec;
    return res;
  }
}

bool Visitor::DeclareMode() const { return declaring_ != ""; }

}  // namespace ast

}  // namespace dbtrain
