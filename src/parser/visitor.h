#ifndef DBTRAIN_VISITOR_H
#define DBTRAIN_VISITOR_H

#include <map>

#include "ast.h"
#include "exec/exec.h"

namespace dbtrain {

namespace ast {

class Visitor {
 public:
  virtual ~Visitor();

  virtual std::any visit(SQL *);
  virtual std::any visit(ShowDatabases *);
  virtual std::any visit(CreateDatabase *);
  virtual std::any visit(UseDatabase *);
  virtual std::any visit(DropDatabase *);

  virtual std::any visit(CreateTable *);
  virtual std::any visit(ShowTables *);
  virtual std::any visit(DescTable *);
  virtual std::any visit(DropTable *);

  virtual std::any visit(Insert *);
  virtual std::any visit(Delete *);
  virtual std::any visit(Update *);
  virtual std::any visit(Select *);

  virtual std::any visit(Explain *);
  virtual std::any visit(Analyze *);

  virtual std::any visit(Declare *);
  virtual std::any visit(EndDeclare *);
  virtual std::any visit(Run *);
  virtual std::any visit(Signal *);
  virtual std::any visit(Wait *);

  virtual std::any visit(Begin *);
  virtual std::any visit(Commit *);
  virtual std::any visit(Abort *);

  virtual std::any visit(Crash *);
  virtual std::any visit(UndoCrashHere *);
  virtual std::any visit(Checkpoint *);
  virtual std::any visit(Flush *);

  virtual std::any visit(SetClause *);

  virtual std::any visit(Value *);
  virtual std::any visit(IntValue *);
  virtual std::any visit(FloatValue *);
  virtual std::any visit(StringValue *);

  virtual std::any visit(ConditionNode *);
  virtual std::any visit(AlgebraConditionNode *);
  virtual std::any visit(EqualConditionNode *);
  virtual std::any visit(LessConditionNode *);
  virtual std::any visit(GreaterConditionNode *);
  virtual std::any visit(JoinConditionNode *);
  virtual std::any visit(AndConditionNode *);
  virtual std::any visit(OrConditionNode *);

 private:
  Executor *executor_ = nullptr;
  RecordList Execute(OperNode *plan);
  std::map<string, Executor *> declared_sqls_;
  std::string declaring_ = "";

  bool DeclareMode() const;
};

}  // namespace ast

}  // namespace dbtrain

#endif  // DBTRAIN_VISITOR_H
