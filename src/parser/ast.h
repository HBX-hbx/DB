#ifndef DBTRAIN_AST_H
#define DBTRAIN_AST_H

#include <any>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "../defines.h"
#include "../record/fields.h"

namespace dbtrain {

namespace ast {

class Visitor;

enum class SvType { SV_TYPE_INT, SV_TYPE_FLOAT, SV_TYPE_STRING };

enum class SvOp { SV_OP_EQ, SV_OP_LT, SV_OP_GT, SV_OP_LE, SV_OP_GE, SV_OP_NE };

enum class SvLogicOp { SV_AND, SV_OR };

class ASTNode {
 public:
  virtual ~ASTNode() = default;
  virtual std::any accept(Visitor *v) = 0;
};

class SQL : public ASTNode {
 public:
  virtual std::any accept(Visitor *v);
};

class ShowDatabases : public SQL {
 public:
  virtual std::any accept(Visitor *v);
};

class CreateDatabase : public SQL {
 public:
  CreateDatabase(std::string db_name) : db_name_(std::move(db_name)) {}
  virtual std::any accept(Visitor *v);
  std::string db_name_;
};

class UseDatabase : public SQL {
 public:
  UseDatabase(std::string db_name) : db_name_(std::move(db_name)) {}
  virtual std::any accept(Visitor *v);
  std::string db_name_;
};

class DropDatabase : public SQL {
 public:
  DropDatabase(std::string db_name, bool if_exists) : db_name_(std::move(db_name)), if_exists_(if_exists) {}
  virtual std::any accept(Visitor *v);
  std::string db_name_;
  bool if_exists_;
};

class TypeLen : public SQL {
 public:
  TypeLen(SvType type, int len) : type_(type), len_(len) {}
  SvType type_;
  size_t len_;
};

class FieldNode : public SQL {
 public:
  FieldNode(std::string col_name, std::shared_ptr<TypeLen> type_len)
      : col_name_(std::move(col_name)), type_len_(std::move(type_len)) {}
  std::string col_name_;
  std::shared_ptr<TypeLen> type_len_;
};

class CreateTable : public SQL {
 public:
  CreateTable(std::string table_name, std::vector<std::shared_ptr<FieldNode>> fields)
      : table_name_(std::move(table_name)), fields_(std::move(fields)) {}
  virtual std::any accept(Visitor *v);
  std::string table_name_;
  std::vector<std::shared_ptr<FieldNode>> fields_;
};

class ShowTables : public SQL {
 public:
  virtual std::any accept(Visitor *v);
};

class DescTable : public SQL {
 public:
  DescTable(std::string table_name) : table_name_(std::move(table_name)) {}
  virtual std::any accept(Visitor *v);
  std::string table_name_;
};

class DropTable : public SQL {
 public:
  DropTable(std::string table_name) : table_name_(std::move(table_name)) {}
  virtual std::any accept(Visitor *v);
  std::string table_name_;
};

class Col : public SQL {
 public:
  Col(std::string table_name, std::string col_name) : table_name_(std::move(table_name)), col_name_(col_name) {}
  virtual std::any accept(Visitor *v);
  std::string table_name_;
  std::string col_name_;
};

class Value : public SQL {
 public:
  virtual std::any accept(Visitor *v);
  FieldNode *field_;
};

class ConditionNode : public SQL {
 public:
  ConditionNode() {}
  virtual std::any accept(Visitor *v);
};

class AlgebraConditionNode : public ConditionNode {
 public:
  AlgebraConditionNode(std::shared_ptr<Col> col, std::shared_ptr<Value> value) : col_(col), value_(value) {}
  virtual std::any accept(Visitor *v);
  std::shared_ptr<Col> col_;
  std::shared_ptr<Value> value_;
};

class EqualConditionNode : public AlgebraConditionNode {
 public:
  EqualConditionNode(std::shared_ptr<Col> col, std::shared_ptr<Value> value) : AlgebraConditionNode(col, value) {}
  virtual std::any accept(Visitor *v);
};

class LessConditionNode : public AlgebraConditionNode {
 public:
  LessConditionNode(std::shared_ptr<Col> col, std::shared_ptr<Value> value) : AlgebraConditionNode(col, value) {}
  virtual std::any accept(Visitor *v);
};

class GreaterConditionNode : public AlgebraConditionNode {
 public:
  GreaterConditionNode(std::shared_ptr<Col> col, std::shared_ptr<Value> value) : AlgebraConditionNode(col, value) {}
  virtual std::any accept(Visitor *v);
};

class JoinConditionNode : public ConditionNode {
 public:
  JoinConditionNode(std::shared_ptr<Col> lhs, std::shared_ptr<Col> rhs) : lhs_(lhs), rhs_(rhs) {}
  virtual std::any accept(Visitor *v);
  std::shared_ptr<Col> lhs_;
  std::shared_ptr<Col> rhs_;
};

class AndConditionNode : public ConditionNode {
 public:
  AndConditionNode(std::shared_ptr<ConditionNode> lhs, std::shared_ptr<ConditionNode> rhs) : lhs_(lhs), rhs_(rhs) {}
  virtual std::any accept(Visitor *v);
  std::shared_ptr<ConditionNode> lhs_;
  std::shared_ptr<ConditionNode> rhs_;
};

class OrConditionNode : public ConditionNode {
 public:
  OrConditionNode(std::shared_ptr<ConditionNode> lhs, std::shared_ptr<ConditionNode> rhs) : lhs_(lhs), rhs_(rhs) {}
  virtual std::any accept(Visitor *v);
  std::shared_ptr<ConditionNode> lhs_;
  std::shared_ptr<ConditionNode> rhs_;
};

class InConditionNode : public ConditionNode {
 public:
  virtual std::any accept(Visitor *v);
};

class IntValue : public Value {
 public:
  IntValue(int val) : val_(val) {}
  virtual std::any accept(Visitor *v);
  int val_;
  //  IntField *int_field_;
};

class FloatValue : public Value {
 public:
  FloatValue(float val) : val_(val) {}
  virtual std::any accept(Visitor *v);
  float val_;
  //  FloatField *float_field_;
};

class StringValue : public Value {
 public:
  StringValue(std::string val) : val_(val) {}
  virtual std::any accept(Visitor *v);
  std::string val_;
  //  StrField *str_field_;
};

class Insert : public SQL {
 public:
  Insert(std::string table_name, std::vector<std::vector<std::shared_ptr<Value>>> vals_list)
      : table_name_(std::move(table_name)), vals_list_(std::move(vals_list)) {}
  virtual std::any accept(Visitor *v);
  std::string table_name_;
  std::vector<std::vector<std::shared_ptr<Value>>> vals_list_;
};

class SetClause : public SQL {
 public:
  SetClause(std::string col_name, std::shared_ptr<Value> val) : col_name_(std::move(col_name)), val_(std::move(val)) {}
  virtual std::any accept(Visitor *v);
  std::string col_name_;
  std::shared_ptr<Value> val_;
};

class Update : public SQL {
 public:
  Update(std::string table_name, std::vector<std::shared_ptr<SetClause>> set_clauses,
         std::shared_ptr<ConditionNode> condition)
      : table_name_(std::move(table_name)), set_clauses_(std::move(set_clauses)), condition_(std::move(condition)) {}
  virtual std::any accept(Visitor *v);
  std::string table_name_;
  std::vector<std::shared_ptr<SetClause>> set_clauses_;
  std::shared_ptr<ConditionNode> condition_;
};

class Select : public SQL {
 public:
  Select(std::vector<std::shared_ptr<Col>> cols, std::vector<std::string> tables,
         std::shared_ptr<ConditionNode> condition)
      : cols_(cols), tables_(tables), condition_(condition) {}
  virtual std::any accept(Visitor *v);
  std::vector<std::shared_ptr<Col>> cols_;
  std::vector<std::string> tables_;
  std::shared_ptr<ConditionNode> condition_;
};

class Delete : public SQL {
 public:
  Delete(std::string table_name, std::shared_ptr<ConditionNode> condition)
      : table_name_(std::move(table_name)), condition_(std::move(condition)) {}
  virtual std::any accept(Visitor *v);
  std::string table_name_;
  std::shared_ptr<ConditionNode> condition_;
};

class Explain : public SQL {
 public:
  Explain(std::shared_ptr<SQL> sql) : sql_(std::move(sql)) {}
  virtual std::any accept(Visitor *v);
  std::shared_ptr<SQL> sql_;
};

class Analyze : public SQL {
 public:
  Analyze() {}
  virtual std::any accept(Visitor *v);
};

class Declare : public SQL {
 public:
  Declare(std::string declaring) : declaring_(declaring) {}
  virtual std::any accept(Visitor *v);
  std::string declaring_;
};

class EndDeclare : public SQL {
 public:
  EndDeclare(std::string declaring) : declaring_(declaring) {}
  virtual std::any accept(Visitor *v);
  std::string declaring_;
};

class Run : public SQL {
 public:
  Run(std::vector<std::string> declares) : declares_(declares) {}
  virtual std::any accept(Visitor *v);
  std::vector<std::string> declares_;
};

class Signal : public SQL {
 public:
  Signal(std::string signal_name) : signal_name_(signal_name) {}
  virtual std::any accept(Visitor *v);
  std::string signal_name_;
};

class Wait : public SQL {
 public:
  Wait(std::string signal_name) : signal_name_(signal_name) {}
  virtual std::any accept(Visitor *v);
  std::string signal_name_;
};

class Begin : public SQL {
 public:
  Begin() {}
  virtual std::any accept(Visitor *v);
};

class Commit : public SQL {
 public:
  Commit() {}
  virtual std::any accept(Visitor *v);
};

class Abort : public SQL {
 public:
  Abort() {}
  virtual std::any accept(Visitor *v);
};

class Crash : public SQL {
 public:
  Crash() {}
  virtual std::any accept(Visitor *v);
};

class Checkpoint : public SQL {
 public:
  Checkpoint() {}
  virtual std::any accept(Visitor *v);
};

class Flush : public SQL {
 public:
  Flush() {}
  virtual std::any accept(Visitor *v);
};

struct SemValue {
  int sv_int;
  float sv_float;
  std::string sv_str;
  bool sv_bool;
  std::vector<std::string> sv_strs;
  SvOp sv_op;
  SvLogicOp sv_logic_op;
  std::shared_ptr<SQL> sv_node;
  std::shared_ptr<FieldNode> sv_field;
  std::vector<std::shared_ptr<FieldNode>> sv_fields;

  std::shared_ptr<Value> sv_val;
  std::vector<std::shared_ptr<Value>> sv_vals;
  std::vector<std::vector<std::shared_ptr<Value>>> sv_vals_list;

  std::shared_ptr<Col> sv_col;
  std::vector<std::shared_ptr<Col>> sv_cols;

  std::shared_ptr<SetClause> sv_set_clause;
  std::vector<std::shared_ptr<SetClause>> sv_set_clauses;

  std::shared_ptr<TypeLen> sv_type_len;

  std::shared_ptr<ConditionNode> sv_condition;
};

extern std::shared_ptr<SQL> parse_tree;

}  // namespace ast

#define YYSTYPE dbtrain::ast::SemValue

}  // namespace dbtrain

#endif  // DBTRAIN_AST_H
