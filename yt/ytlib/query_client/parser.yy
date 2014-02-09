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
%parse-param {TPlanContext* context}
%parse-param {const TOperator** head}

%code requires {
    #include "plan_node.h"

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
%token KwGroupBy "keyword `GROUP BY`"
%token KwAs "keyword `AS`"

%token KwAnd "keyword `AND`"
%token KwOr "keyword `OR`"

%token <TStringBuf> Identifier "identifier"

%token <i64> IntegerLiteral "integer literal"
%token <double> DoubleLiteral "double literal"
%token <TStringBuf> YPathLiteral "YPath literal"

%token OpModulo 37 "`%`"

%token LeftParenthesis 40 "`(`"
%token RightParenthesis 41 "`)`"

%token Asterisk 42 "`*`"
%token OpPlus 43 "`+`"
%token Comma 44 "`,`"
%token OpMinus 45 "`-`"
%token OpDivide 47 "`/`"

%token OpLess 60 "`<`"
%token OpLessOrEqual "`<=`"
%token OpEqual 61 "`=`"
%token OpNotEqual "`!=`"
%token OpGreater 62 "`>`"
%token OpGreaterOrEqual "`>=`"

%type <TOperator*> select-clause
%type <TOperator*> from-where-clause
%type <TOperator*> from-clause

%type <TNamedExpressionList> named-expression-list
%type <TNamedExpression> named-expression

%type <TExpression*> expression
%type <TExpression*> relational-op-expr
%type <TExpression*> or-op-expr
%type <TExpression*> and-op-expr
%type <TExpression*> equality-op-expr
%type <TExpression*> multiplicative-op-expr
%type <TExpression*> additive-op-expr
%type <TExpression*> atomic-expr
%type <TFunctionExpression*> function-expr
%type <TFunctionExpression::TArguments> function-expr-args

%type <TReferenceExpression*> reference-expr

%type <EBinaryOp> equality-op
%type <EBinaryOp> relational-op
%type <EBinaryOp> multiplicative-op
%type <EBinaryOp> additive-op

%start head

%%

head
    : select-clause
        {
            *head = $[select-clause];
        }
;

select-clause
    : named-expression-list[projections] from-where-clause[source]
        {
            auto projectOp = new (context) TProjectOperator(context, $source);
            projectOp->Projections().assign($projections.begin(), $projections.end());
            $$ = projectOp;
        }
;

from-where-clause
    : from-clause[source]
        {
            $$ = $source;
        }
    | from-clause[source] KwWhere or-op-expr[predicate]
        {
            auto filterOp = new (context) TFilterOperator(context, $source);
            filterOp->SetPredicate($predicate);

            $$ = filterOp;
        }
    | from-clause[source] KwWhere or-op-expr[predicate] KwGroupBy named-expression-list[exprs]
        {
            auto filterOp = new (context) TFilterOperator(context, $source);
            filterOp->SetPredicate($predicate);

            auto groupOp = new (context) TGroupOperator(context, filterOp);
            groupOp->GroupItems().assign($exprs.begin(), $exprs.end());

            $$ = groupOp;
        }
    | from-clause[source] KwGroupBy named-expression-list[exprs]
        {
            auto groupOp = new (context) TGroupOperator(context, $source);
            groupOp->GroupItems().assign($exprs.begin(), $exprs.end());

            $$ = groupOp;
        }
;

from-clause
    : KwFrom YPathLiteral[path]
        {
            context->TableDescriptor().Path = $path;

            auto scanOp = new (context) TScanOperator(context);

            $$ = scanOp;
        }
;

named-expression-list
    : named-expression-list[ps] Comma named-expression[p]
        {
            $$.swap($ps);
            $$.push_back($p);
        }
    | named-expression[p]
        {
            $$.push_back($p);
        }
;

named-expression
    : reference-expr
        {
            $$ = TNamedExpression($1, $1->GetColumnName());
        }
    | expression[expr] KwAs Identifier[name]
        {
            $$ = TNamedExpression($expr, Stroka($name));
        }
;

expression
    : or-op-expr
        { $$ = $1; }
;

or-op-expr
    : or-op-expr[lhs] KwOr and-op-expr[rhs]
        {
            $$ = new (context) TBinaryOpExpression(
                context,
                @$,
                EBinaryOp::Or,
                $lhs,
                $rhs);
        }
    | and-op-expr
        { $$ = $1; }
;

and-op-expr
    : and-op-expr[lhs] KwAnd equality-op-expr[rhs]
        {
            $$ = new (context) TBinaryOpExpression(
                context,
                @$,
                EBinaryOp::And,
                $lhs,
                $rhs);
        }
    | equality-op-expr
        { $$ = $1; }
;

equality-op-expr
    : additive-op-expr[lhs] equality-op[opcode] relational-op-expr[rhs]
        {
            $$ = new (context) TBinaryOpExpression(
                context,
                @$,
                $opcode,
                $lhs,
                $rhs);
        }
    | relational-op-expr
        { $$ = $1; }
;

equality-op
    : OpEqual
        { $$ = EBinaryOp::Equal; }
    | OpNotEqual
        { $$ = EBinaryOp::NotEqual; }
;

relational-op-expr
    : relational-op-expr[lhs] relational-op[opcode] additive-op-expr[rhs]
        {
            $$ = new (context) TBinaryOpExpression(
                context,
                @$,
                $opcode,
                $lhs,
                $rhs);
        }
    | additive-op-expr
        { $$ = $1; }
;

relational-op
    : OpLess
        { $$ = EBinaryOp::Less; }
    | OpLessOrEqual
        { $$ = EBinaryOp::LessOrEqual; }
    | OpGreater
        { $$ = EBinaryOp::Greater; }
    | OpGreaterOrEqual
        { $$ = EBinaryOp::GreaterOrEqual; }
;

additive-op-expr
    : additive-op-expr[lhs] additive-op[opcode] multiplicative-op-expr[rhs]
        {
            $$ = new (context) TBinaryOpExpression(
                context,
                @$,
                $opcode,
                $lhs,
                $rhs);
        }
    | multiplicative-op-expr
        { $$ = $1; }
;

additive-op
    : OpPlus
        { $$ = EBinaryOp::Plus; }
    | OpMinus
        { $$ = EBinaryOp::Minus; }
;

multiplicative-op-expr
    : multiplicative-op-expr[lhs] multiplicative-op[opcode] atomic-expr[rhs]
        {
            $$ = new (context) TBinaryOpExpression(
                context,
                @$,
                $opcode,
                $lhs,
                $rhs);
        }
    | atomic-expr
        { $$ = $1; }
;

multiplicative-op
    : Asterisk
        { $$ = EBinaryOp::Multiply; }
    | OpDivide
        { $$ = EBinaryOp::Divide; }
    | OpModulo
        { $$ = EBinaryOp::Modulo; }
;

atomic-expr
    : reference-expr
        { $$ = $1; }
    | IntegerLiteral[value]
        {
            $$ = new (context) TIntegerLiteralExpression(context, @$, $value);
        }
    | DoubleLiteral[value]
        {
            $$ = new (context) TDoubleLiteralExpression(context, @$, $value);
        }
    | LeftParenthesis or-op-expr[expr] RightParenthesis
        { $$ = $expr; }
    | function-expr
        { $$ = $1; }
;

function-expr
    : Identifier[name] LeftParenthesis function-expr-args[args] RightParenthesis
        {
            $$ = new (context) TFunctionExpression(
                context,
                @$,
                $name);
            $$->Arguments().assign($args.begin(), $args.end());
        }
;

reference-expr
    : Identifier[name]
        {
            $$ = new (context) TReferenceExpression(
                context,
                @$,
                $name);
        }
;

function-expr-args
    : function-expr-args[as] Comma expression[a]
        {
            $$.swap($as);
            $$.push_back($a);
        }
    | expression[a]
        {
            $$.push_back($a);
        }
;

%%

namespace NYT {
namespace NQueryClient {

const TSourceLocation NullSourceLocation = { 0, 0 };

void TParser::error(const location_type& location, const std::string& message)
{
    THROW_ERROR_EXCEPTION("Error while parsing query: %s", message.c_str())
        << TErrorAttribute("location", location.ToString());
}

} // namespace NQueryClient
} // namespace NYT

