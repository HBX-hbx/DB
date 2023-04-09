%{
#include "ast.h"
#include "sql.tab.h"
#include <iostream>
#include <memory>

int yylex();
void yyerror(const char *s);

using namespace dbtrain::ast;
%}

%token EXPLAIN ANALYZE
%token INSERT DELETE UPDATE SELECT
%token CREATE DROP USE SHOW DESC
%token DATABASES DATABASE TABLES TABLE
%token INT_ FLOAT_ CHAR VARCHAR
%token INTO VALUES FROM WHERE SET
%token AND OR
%token IN
%token IF NOT EXISTS
%token SUM AVG MAX MIN COUNT
%token BEGIN_ COMMIT ABORT
%token CRASH UNDO_CRASH_HERE CHECKPOINT FLUSH
%token RUN DECLARE ENDDECL SIGNAL_ WAIT_
%token T_EOF

%token <sv_int> VALUE_INT
%token <sv_float> VALUE_FLOAT
%token <sv_str> VALUE_STRING IDENTIFIER

%type <sv_node> stmt select_stmt explainable_stmt
%type <sv_bool> opt_if_exists
%type <sv_field> field
%type <sv_fields> field_list
%type <sv_strs> identifiers
%type <sv_cols> selectors selector_list
%type <sv_val> value
%type <sv_col> col selector
%type <sv_vals> values
%type <sv_vals_list> value_lists
%type <sv_set_clause> set_clause
%type <sv_set_clauses> set_clauses
%type <sv_type_len> col_type
%type <sv_condition> condition conditions algebra_condition join_condition where_clause

%left OR
%left AND

%%

sql:
        stmt ';'
        {
            parse_tree = $1;
            YYACCEPT;
        }
    |   T_EOF
        {
            parse_tree = nullptr;
            YYACCEPT;
        }
    ;

stmt:
        SHOW DATABASES
        {
            $$ = std::make_shared<ShowDatabases>();
        }
    |   CREATE DATABASE IDENTIFIER
        {
            $$ = std::make_shared<CreateDatabase>($3);
        }
    |   USE IDENTIFIER
        {
            $$ = std::make_shared<UseDatabase>($2);
        }
    |   DROP DATABASE opt_if_exists IDENTIFIER
        {
            $$ = std::make_shared<DropDatabase>($4, $3);
        }
    |   CREATE TABLE IDENTIFIER '(' field_list ')'
        {
            $$ = std::make_shared<CreateTable>($3, $5);
        }
    |   SHOW TABLES
        {
            $$ = std::make_shared<ShowTables>();
        }
    |   DESC IDENTIFIER
        {
            $$ = std::make_shared<DescTable>($2);
        }
    |   DROP TABLE IDENTIFIER
        {
            $$ = std::make_shared<DropTable>($3);
        }
    |   BEGIN_
        {
            $$ = std::make_shared<Begin>();
        }
    |   COMMIT
        {
            $$ = std::make_shared<Commit>();
        }
    |   ABORT
        {
            $$ = std::make_shared<Abort>();
        }
    |   CRASH
        {
            $$ = std::make_shared<Crash>();
        }
    |   UNDO_CRASH_HERE
        {
            $$ = std::make_shared<UndoCrashHere>();
        }
    |   CHECKPOINT
        {
            $$ = std::make_shared<Checkpoint>();
        }
    |   FLUSH
        {
            $$ = std::make_shared<Flush>();
        }
    |   INSERT INTO IDENTIFIER VALUES value_lists
        {
            $$ = std::make_shared<Insert>($3, $5);
        }
    |   DELETE FROM IDENTIFIER where_clause
        {
            $$ = std::make_shared<Delete>($3, $4);
        }
    |   UPDATE IDENTIFIER SET set_clauses where_clause
        {
            $$ = std::make_shared<Update>($2, $4, $5);
        }
    |   select_stmt
        {
            $$ = $1;
        }
    |   EXPLAIN explainable_stmt
        {
            $$ = std::make_shared<Explain>($2);
        }
    |   ANALYZE
        {
            $$ = std::make_shared<Analyze>();
        }
    |   DECLARE IDENTIFIER
        {
            $$ = std::make_shared<Declare>($2);
        }
    |   ENDDECL IDENTIFIER
        {
            $$ = std::make_shared<EndDeclare>($2);
        }
    |   RUN identifiers
        {
            $$ = std::make_shared<Run>($2);
        }
    |   SIGNAL_ IDENTIFIER
        {
            $$ = std::make_shared<Signal>($2);
        }
    |   WAIT_ IDENTIFIER
        {
            $$ = std::make_shared<Wait>($2);
        }
    ;

opt_if_exists:
        /* */
        {
            $$ = false;
        }
    |   IF EXISTS
        {
            $$ = true;
        }
    ;


explainable_stmt:
        select_stmt
        {
            $$ = $1;
        }
    ;

select_stmt:
        SELECT selectors FROM identifiers where_clause
        {
            $$ = std::make_shared<Select>($2, $4, $5);
        }
        

set_clauses:
        set_clause
        {
            $$ = std::vector<std::shared_ptr<SetClause>>{$1};
        }
    |   set_clauses ',' set_clause
        {
            $$.push_back($3);
        }
    ;

set_clause:
        IDENTIFIER '=' value
        {
            $$ = std::make_shared<SetClause>($1, $3);
        }

identifiers:
        IDENTIFIER
        {
            $$ = std::vector<std::string>{$1};
        }
    |   identifiers ',' IDENTIFIER
        {
            $$.push_back($3);
        }
    ;

field_list:
        field
        {
            $$ = std::vector<std::shared_ptr<FieldNode>>{$1};
        }
    |   field_list ',' field
        {
            $$.push_back($3);
        }
    ;

field:
        IDENTIFIER col_type
        {
            $$ = std::make_shared<FieldNode>($1, $2);
        }
    ;

col_type:
        INT_
        {
            $$ = std::make_shared<TypeLen>(SvType::SV_TYPE_INT, sizeof(int));
        }
    |   FLOAT_
        {
            $$ = std::make_shared<TypeLen>(SvType::SV_TYPE_FLOAT, sizeof(double));
        }
    |   CHAR '(' VALUE_INT ')'
        {
            $$ = std::make_shared<TypeLen>(SvType::SV_TYPE_STRING, $3);
        }
    |   VARCHAR '(' VALUE_INT ')'
        {
            $$ = std::make_shared<TypeLen>(SvType::SV_TYPE_STRING, $3);
        }
    ;

value_lists:
        '(' values ')'
        {
            $$ = std::vector<std::vector<std::shared_ptr<Value>>>{$2};
        }
    |   value_lists ',' '(' values ')'
        {
            $$.push_back($4);
        }
    ;

values:
        value
        {
            $$ = std::vector<std::shared_ptr<Value>>{$1};
        }
    |   values ',' value
        {
            $$.push_back($3);
        }
    ;

selectors:
        '*'
        {
            $$ = {};
        }
    |   selector_list
        {

        }
    ;

selector_list:
        selector
        {
            $$ = std::vector<std::shared_ptr<Col>>{$1};
        }
    |   selector_list ',' selector
        {
            $$.push_back($3);
        }
    ;

selector:
        col
        {

        }
    |   aggregator
        {

        }
    ;

aggregator:
        SUM '(' col ')'
        {

        }
    |   AVG '(' col ')'
        {

        }
    |   MIN '(' col ')'
        {

        }
    |   MAX '(' col ')'
        {

        }
    |   COUNT '(' col ')'
        {

        }
    |   COUNT '(' '*' ')'
        {

        }
    ;

where_clause:
        WHERE conditions
        {
            $$ = $2;
        }
    |   /* ignore */
        {
            /* ignore */
        }
    ;

conditions:
        condition
        {

        }
    |   conditions AND condition
        {
            $$ = std::make_shared<AndConditionNode>($1, $3);
        }
    |   conditions OR condition
        {
            $$ = std::make_shared<OrConditionNode>($1, $3);
        }
    ;

condition:
        algebra_condition
        {

        }
    |   join_condition
        {

        }
    |   in_condition
        {

        }
    ;

algebra_condition:
        col '=' value
        {
            $$ = std::make_shared<EqualConditionNode>($1, $3);
        }
    |   col '<' value
        {
            $$ = std::make_shared<LessConditionNode>($1, $3);
        }
    |   col '>' value
        {
            $$ = std::make_shared<GreaterConditionNode>($1, $3);
        }
    ;

join_condition:
        col '=' col
        {
            $$ = std::make_shared<JoinConditionNode>($1, $3);
        }
    ;

in_condition:
        col IN '(' values ')'
        {

        }
    ;

value:
        VALUE_INT
        {
            $$ = std::make_shared<IntValue>($1);
        }
    |   VALUE_FLOAT
        {
            $$ = std::make_shared<FloatValue>($1);
        }
    |   VALUE_STRING
        {
            $$ = std::make_shared<StringValue>($1);
        }
    ;

col:
        IDENTIFIER
        {
            $$ = std::make_shared<Col>("", $1);
        }
    |   IDENTIFIER '.' IDENTIFIER
        {
            $$ = std::make_shared<Col>($1, $3);
        }
    ;

%%
void yyerror(const char *s) {
    std::cerr << "Error " << s << std::endl;
}
