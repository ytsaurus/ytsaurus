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

    namespace NYT { namespace NQueryClient {
        using namespace NVersionedTableClient;

        class TLexer;
        class TParser;
    } }
}

%code {
    #include <ytlib/query_client/lexer.h>
    #define yt_ql_yylex lexer.GetNextToken

    #ifndef YYLLOC_DEFAULT
    #define YYLLOC_DEFAULT(Current, Rhs, N) \
        do { \
            if (N) { \
                (Current).first = YYRHSLOC(Rhs, 1).first; \
                (Current).second = YYRHSLOC (Rhs, N).second; \
            } else { \
                (Current).first = (Current).second = YYRHSLOC(Rhs, 0).second; \
            } \
        } while (false)
    #endif
}

// Special stray tokens to control parser flow.

// NB: Enumerate stray tokens in decreasing order, e. g. 999, 998, and so on
//     so that actual tokens won't change their identifiers.
// NB: And keep one-character tokens consistent with their ASCII codes
//     to simplify lexing.

%token End 0 "end of stream"
%token Failure 256 "lexer failure"

%token StrayWillParseQuery 999

// Language tokens.

%token KwFrom "keyword `FROM`"
%token KwWhere "keyword `WHERE`"
%token KwGroupBy "keyword `GROUP BY`"
%token KwAs "keyword `AS`"

%token KwAnd "keyword `AND`"
%token KwOr "keyword `OR`"
%token KwBetween "keyword `BETWEEN`"
%token KwIn "keyword `IN`"

%token <TStringBuf> Identifier "identifier"

%token <TUnversionedValue> Int64Literal "int64 literal"
%token <TUnversionedValue> Uint64Literal "uint64 literal"
%token <TUnversionedValue> DoubleLiteral "double literal"
%token <TUnversionedValue> StringLiteral "string literal"

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

%type <TOperator*> head-clause
%type <TProjectOperator*> select-clause
%type <TScanOperator*> from-clause
%type <TFilterOperator*> where-clause
%type <TGroupOperator*> group-by-clause

%type <TNamedExpressionList> named-expression-list
%type <TNamedExpression> named-expression

%type <TExpression*> expression
%type <TExpression*> or-op-expr
%type <TExpression*> and-op-expr
%type <TExpression*> relational-op-expr
%type <TExpression*> multiplicative-op-expr
%type <TExpression*> additive-op-expr
%type <TExpression*> atomic-expr
%type <TReferenceExpression*> reference-expr
%type <TFunctionExpression*> function-expr
%type <TFunctionExpression::TArguments> function-expr-args

%type <EBinaryOp> relational-op
%type <EBinaryOp> multiplicative-op
%type <EBinaryOp> additive-op

%start head

%%

head
    : StrayWillParseQuery head-clause
        {
            *head = $[head-clause];
        }
;

head-clause
    : select-clause[project] from-clause[scan]
        {
            $project->SetSource($scan);
            $$ = $project;
        }
    | select-clause[project] from-clause[scan] where-clause[filter]
        {
            $filter->SetSource($scan);
            $project->SetSource($filter);
            $$ = $project;
        }
    | select-clause[project] from-clause[scan] where-clause[filter] group-by-clause[group]
        {
            $filter->SetSource($scan);
            $group->SetSource($filter);
            $project->SetSource($group);
            $$ = $project;
        }
    | select-clause[project] from-clause[scan] group-by-clause[group]
        {
            $group->SetSource($scan);
            $project->SetSource($group);
            $$ = $project;
        }
;

select-clause
    : named-expression-list[projections]
        {
            $$ = context->TrackedNew<TProjectOperator>(nullptr);
            $$->Projections().assign($projections.begin(), $projections.end());
        }
;

from-clause
    : KwFrom Identifier[path]
        {
            context->SetTablePath(Stroka(~$path, +$path));

            $$ = context->TrackedNew<TScanOperator>();
        }
;

where-clause
    : KwWhere or-op-expr[predicate]
        {
            $$ = context->TrackedNew<TFilterOperator>(nullptr);
            $$->SetPredicate($predicate);
        }
;

group-by-clause
    : KwGroupBy named-expression-list[exprs]
        {
            $$ = context->TrackedNew<TGroupOperator>(nullptr);
            $$->GroupItems().assign($exprs.begin(), $exprs.end());
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
            $$ = context->TrackedNew<TBinaryOpExpression>(@$, EBinaryOp::Or, $lhs, $rhs);
        }
    | and-op-expr
        { $$ = $1; }
;

and-op-expr
    : and-op-expr[lhs] KwAnd relational-op-expr[rhs]
        {
            $$ = context->TrackedNew<TBinaryOpExpression>(@$, EBinaryOp::And, $lhs, $rhs);
        }
    | relational-op-expr
        { $$ = $1; }
;

relational-op-expr
    : relational-op-expr[lhs] relational-op[opcode] additive-op-expr[rhs]
        {
            $$ = context->TrackedNew<TBinaryOpExpression>(@$, $opcode, $lhs, $rhs);
        }
    | atomic-expr[expr] KwBetween atomic-expr[lbexpr] KwAnd atomic-expr[rbexpr]
        {
            $$ = context->TrackedNew<TBinaryOpExpression>(@$, EBinaryOp::And, 
                context->TrackedNew<TBinaryOpExpression>(@$, EBinaryOp::GreaterOrEqual, $expr, $lbexpr), 
                context->TrackedNew<TBinaryOpExpression>(@$, EBinaryOp::LessOrEqual, $expr, $rbexpr));
        }
    | atomic-expr[expr] KwIn LeftParenthesis function-expr-args[args] RightParenthesis
        {
            $$ = context->TrackedNew<TLiteralExpression>(@$, MakeUnversionedBooleanValue(false));

            for (const TExpression* current : $args) {
                $$ = context->TrackedNew<TBinaryOpExpression>(
                    @$,
                    EBinaryOp::Or,
                    context->TrackedNew<TBinaryOpExpression>(@$, EBinaryOp::Equal, $expr, current),
                    $$);
            }
        }
    | additive-op-expr
        { $$ = $1; }
;

relational-op
    : OpEqual
        { $$ = EBinaryOp::Equal; }
    | OpNotEqual
        { $$ = EBinaryOp::NotEqual; }
    | OpLess
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
            $$ = context->TrackedNew<TBinaryOpExpression>(@$, $opcode, $lhs, $rhs);
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
            $$ = context->TrackedNew<TBinaryOpExpression>(@$, $opcode, $lhs, $rhs);
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
    | function-expr
        { $$ = $1; }
    | Int64Literal[value]
        {
            $$ = context->TrackedNew<TLiteralExpression>(@$, $value);
        }
    | Uint64Literal[value]
        {
            $$ = context->TrackedNew<TLiteralExpression>(@$, $value);
        }
    | DoubleLiteral[value]
        {
            $$ = context->TrackedNew<TLiteralExpression>(@$, $value);
        }
    | StringLiteral[value]
        {
            $$ = context->TrackedNew<TLiteralExpression>(@$, $value);
        }
    | LeftParenthesis or-op-expr[expr] RightParenthesis
        {
            $$ = $expr;
        }
;

reference-expr
    : Identifier[name]
        {
            $$ = context->TrackedNew<TReferenceExpression>(@$, $name);
        }
;

function-expr
    : Identifier[name] LeftParenthesis function-expr-args[args] RightParenthesis
        {
            $$ = context->TrackedNew<TFunctionExpression>(@$, $name);
            $$->Arguments().assign($args.begin(), $args.end());
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

#include <core/misc/format.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void TParser::error(const location_type& location, const std::string& message)
{
    THROW_ERROR_EXCEPTION("Error while parsing query: %s", message.c_str())
        << TErrorAttribute("query_range", Format("%v-%v", location.first, location.second));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
