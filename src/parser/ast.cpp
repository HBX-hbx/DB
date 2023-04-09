#include "ast.h"

#include "visitor.h"

namespace dbtrain {

namespace ast {

std::shared_ptr<SQL> parse_tree;

std::any SQL::accept(Visitor *v) { return v->visit(this); }
std::any ShowDatabases::accept(Visitor *v) { return v->visit(this); }
std::any CreateDatabase::accept(Visitor *v) { return v->visit(this); }
std::any UseDatabase::accept(Visitor *v) { return v->visit(this); }
std::any DropDatabase::accept(Visitor *v) { return v->visit(this); }
std::any CreateTable::accept(Visitor *v) { return v->visit(this); }
std::any ShowTables::accept(Visitor *v) { return v->visit(this); }
std::any DescTable::accept(Visitor *v) { return v->visit(this); }
std::any DropTable::accept(Visitor *v) { return v->visit(this); }
std::any Col::accept(Visitor *v) { return v->visit(this); }
std::any Insert::accept(Visitor *v) { return v->visit(this); }
std::any Delete::accept(Visitor *v) { return v->visit(this); }
std::any Update::accept(Visitor *v) { return v->visit(this); }
std::any Select::accept(Visitor *v) { return v->visit(this); }
std::any Explain::accept(Visitor *v) { return v->visit(this); }
std::any Analyze::accept(Visitor *v) { return v->visit(this); }
std::any Declare::accept(Visitor *v) { return v->visit(this); }
std::any EndDeclare::accept(Visitor *v) { return v->visit(this); }
std::any Run::accept(Visitor *v) { return v->visit(this); }
std::any Signal::accept(Visitor *v) { return v->visit(this); }
std::any Wait::accept(Visitor *v) { return v->visit(this); }
std::any Begin::accept(Visitor *v) { return v->visit(this); }
std::any Commit::accept(Visitor *v) { return v->visit(this); }
std::any Abort::accept(Visitor *v) { return v->visit(this); }
std::any Crash::accept(Visitor *v) { return v->visit(this); }
std::any UndoCrashHere::accept(Visitor *v) { return v->visit(this); }
std::any Checkpoint::accept(Visitor *v) { return v->visit(this); }
std::any Flush::accept(Visitor *v) { return v->visit(this); }

std::any SetClause::accept(Visitor *v) { return v->visit(this); }
std::any Value::accept(Visitor *v) { return v->visit(this); }
std::any IntValue::accept(Visitor *v) { return v->visit(this); }
std::any FloatValue::accept(Visitor *v) { return v->visit(this); }
std::any StringValue::accept(Visitor *v) { return v->visit(this); }

std::any ConditionNode::accept(Visitor *v) { return v->visit(this); }
std::any AlgebraConditionNode::accept(Visitor *v) { return v->visit(this); }
std::any EqualConditionNode::accept(Visitor *v) { return v->visit(this); }
std::any LessConditionNode::accept(Visitor *v) { return v->visit(this); }
std::any GreaterConditionNode::accept(Visitor *v) { return v->visit(this); }
std::any JoinConditionNode::accept(Visitor *v) { return v->visit(this); }
std::any AndConditionNode::accept(Visitor *v) { return v->visit(this); }
std::any OrConditionNode::accept(Visitor *v) { return v->visit(this); }

}  // namespace ast

}  // namespace dbtrain
