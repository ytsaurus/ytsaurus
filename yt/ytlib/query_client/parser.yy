%skeleton "lalr1.cc"
%require "3.0"
%language "C++"

%define api.namespace {NYT::NQueryClient}
%define api.prefix {yt_ql_yy}
%define api.value.type variant
%define api.location.type {TSourceLocation}
%define parser_class_name {TParser}
%define parse.error verbose

%defines
%locations

%parse-param {TLexer& lexer}
%parse-param {TQueryContext* context}
%parse-param {TOperator** head}

%code requires {
    #include <ytlib/query_client/ast.h>

    #include <cmath>
    #include <iostream>

    namespace NYT { namespace NQueryClient {
        class TLexer;
        class TParser;
    } }
}

%code {
    #include <ytlib/query_client/lexer.h>
    #define yt_ql_yylex lexer.GetNextToken
}

%token End 0 "end of stream"
%token Failure 256 "lexer failure"

// NB: Keep one-character tokens consistent with ASCII codes to simplify lexing.

%token KwFrom "keyword `FROM`"
%token KwWhere "keyword `WHERE`"

%token <TStringBuf> Identifier "identifier"

%token <i64> IntegerLiteral "integer literal"
%token <double> DoubleLiteral "double literal"
%token <TStringBuf> YPathLiteral "YPath literal"

%token LeftParenthesis 40 "`(`"
%token RightParenthesis 41 "`)`"

%token Asterisk 42 "`*`"
%token Comma 44 "`,`"

%token OpLess 60 "`<`"
%token OpLessOrEqual "`<=`"
%token OpEqual 61 "`=`"
%token OpNotEqual "`!=`"
%token OpGreater 62 "`>`"
%token OpGreaterOrEqual "`>=`"

%type <TProjectOperator*> select-clause
%type <TScanOperator*> from-clause
%type <TFilterOperator*> where-clause

%type <TSmallVector<TExpression*, TypicalProjectExpressionCount>> select-exprs
%type <TExpression*> select-expr

%type <TExpression*> atomic-expr

%type <TExpression*> function-expr
%type <TSmallVector<TExpression*, TypicalExpressionChildCount>> function-arg-exprs
%type <TExpression*> function-arg-expr

%type <TExpression*> binary-rel-op-expr
%type <EBinaryOp> binary-rel-op

%start query

%%

query
    : select-clause from-clause where-clause
        {
            $[where-clause]->AttachChild($[from-clause]);
            $[select-clause]->AttachChild($[where-clause]);
            *head = $[select-clause];
        }
;

select-clause
    : select-exprs[exprs]
        {
            $$ = new(context) TProjectOperator(
                context,
                $exprs.begin(),
                $exprs.end());
        }
;

select-exprs
    : select-exprs[exprs] Comma select-expr[expr]
        {
            $$.swap($exprs);
            $$.push_back($expr);
        }
    | select-expr[expr]
        {
            $$.push_back($expr);
        }
;

select-expr
    : atomic-expr
        { $$ = $1; }
    | function-expr
        { $$ = $1; }
;

from-clause
    : KwFrom YPathLiteral[path]
        {
            auto tableIndex = context->GetTableIndexByAlias("");
            $$ = new(context) TScanOperator(context, tableIndex);
            context->BindToTableIndex(tableIndex, $path, $$);
        }
;

where-clause
    : KwWhere binary-rel-op-expr[predicate]
        {
            $$ = new(context) TFilterOperator(
                context,
                $predicate);
        }
;

atomic-expr
    : Identifier[name]
        {
            auto tableIndex = context->GetTableIndexByAlias("");
            $$ = new(context) TReferenceExpression(
                context,
                @$,
                tableIndex,
                $name);
        }
    | IntegerLiteral[value]
        {
            $$ = new(context) TIntegerLiteralExpression(context, @$, $value);
        }
    | DoubleLiteral[value]
        {
            $$ = new(context) TDoubleLiteralExpression(context, @$, $value);
        }
;

function-expr
    : Identifier[name] LeftParenthesis function-arg-exprs[exprs] RightParenthesis
        {
            $$ = new(context) TFunctionExpression(
                context,
                @$,
                $name,
                $exprs.begin(),
                $exprs.end());
        }
;

function-arg-exprs
    : function-arg-exprs[exprs] Comma function-arg-expr[expr]
        {
            $$.swap($exprs);
            $$.push_back($expr);
        }
    | function-arg-expr[expr]
        {
            $$.push_back($expr);
        }
;

function-arg-expr
    : atomic-expr
        { $$ = $1; }
;

binary-rel-op-expr
    : atomic-expr[lhs] binary-rel-op[opcode] atomic-expr[rhs]
        {
            $$ = new(context) TBinaryOpExpression(
                context,
                @$,
                $opcode,
                $lhs,
                $rhs);
        }
;

binary-rel-op
    : OpLess
        { $$ = EBinaryOp::Less; }
    | OpLessOrEqual
        { $$ = EBinaryOp::LessOrEqual; }
    | OpEqual
        { $$ = EBinaryOp::Equal; }
    | OpNotEqual
        { $$ = EBinaryOp::NotEqual; }
    | OpGreater
        { $$ = EBinaryOp::Greater; }
    | OpGreaterOrEqual
        { $$ = EBinaryOp::GreaterOrEqual; }
;

%%

namespace NYT {
namespace NQueryClient {

const TSourceLocation NullSourceLocation = { 0, 0 };

void TParser::error(const location_type& location, const std::string& message)
{
    // TODO(sandello): Better diagnostics.
    std::cerr << message << std::endl;
}

} // namespace NQueryClient
} // namespace NYT

