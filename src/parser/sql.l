%option caseless
%option noyywrap

%{
#include "ast.h"
#include "sql.tab.h"
#include <iostream>
%}

alpha [a-zA-Z]
digit [0-9]
space [ \t]+
newline "\r"|"\n"|"\r\n"
comment "--".*|#.*
sign "+"|"-"
value_int {sign}?{digit}+
value_float {sign}?{digit}+\.{digit}*
value_string '[^']*'
identifier {alpha}(_|{alpha}|{digit})*
single_op ";"|"("|")"|","|"*"|"="|">"|"<"|"."

%%
{newline}       {}
{space}         {}
{comment}       {}
SHOW            { return SHOW; }
DATABASES       { return DATABASES; }
CREATE          { return CREATE; }
DATABASE        { return DATABASE; }
USE             { return USE; }
DROP            { return DROP; }
TABLES          { return TABLES; }
TABLE           { return TABLE; }
DESC            { return DESC; }
INSERT          { return INSERT; }
INTO            { return INTO; }
VALUES          { return VALUES; }
DELETE          { return DELETE; }
UPDATE          { return UPDATE; }
SET             { return SET; }
SELECT          { return SELECT; }
EXPLAIN         { return EXPLAIN; }
FROM            { return FROM; }
INT             { return INT_; }
FLOAT           { return FLOAT_; }
CHAR            { return CHAR; }
VARCHAR         { return VARCHAR; }
AND             { return AND; }
OR              { return OR; }
IN              { return IN; }
WHERE           { return WHERE; }
IF              { return IF; }
NOT             { return NOT; }
EXISTS          { return EXISTS; }
BEGIN           { return BEGIN_; }
COMMIT          { return COMMIT; }
ABORT           { return ABORT; }
CRASH           { return CRASH; }
UNDO_CRASH_HERE { return UNDO_CRASH_HERE; }
FLUSH           { return FLUSH; }
CHECKPOINT      { return CHECKPOINT; }
ANALYZE         { return ANALYZE; }
DECLARE         { return DECLARE; }
ENDDECL         { return ENDDECL; }
RUN             { return RUN; }
SIGNAL          { return SIGNAL_; }
WAIT            { return WAIT_; }
{single_op}     { return yytext[0]; }
{identifier}    { yylval.sv_str = yytext; return IDENTIFIER; }
{value_int}     { yylval.sv_int = atoi(yytext); return VALUE_INT; }
{value_float}   { yylval.sv_float = atof(yytext); return VALUE_FLOAT; }
{value_string}  { yylval.sv_str = std::string(yytext + 1, strlen(yytext) - 2); return VALUE_STRING; }
<<EOF>>         { return T_EOF; }
.               { std::cerr << "Unexpected character " << yytext[0] << std::endl; }
%%
