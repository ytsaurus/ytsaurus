%skeleton "lalr1.cc"
%require "3.0"
%language "C++"

%define api.namespace {NYT::NQueryClient::NAst}
%define api.prefix {yt_ql_yy}
%define api.value.type variant
%define api.location.type {TSourceLocation}
%define parser_class_name {TParser}
%define parse.error verbose

%defines
%locations

%parse-param {TLexer& lexer}
%parse-param {TQuery* head}

%code requires {
    #include "ast.h"

    namespace NYT { namespace NQueryClient { namespace NAst {
        using namespace NVersionedTableClient;

        class TLexer;
        class TParser;
    } } }
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

%type <TNullableNamedExprs> select-clause
%type <TStringBuf> from-clause
%type <TExpressionPtr> where-clause
%type <TNamedExpressionList> group-by-clause

%type <TNamedExpressionList> named-expression-list
%type <TNamedExpression> named-expression

%type <TExpressionPtr> expression
%type <TExpressionPtr> or-op-expr
%type <TExpressionPtr> and-op-expr
%type <TExpressionPtr> relational-op-expr
%type <TExpressionPtr> multiplicative-op-expr
%type <TExpressionPtr> additive-op-expr
%type <TExpressionPtr> atomic-expr
%type <TExpressionPtr> comma-expr
%type <TUnversionedValue> literal-expr
%type <TValueList> literal-list
%type <TValueList> literal-tuple
%type <TValueTupleList> literal-tuple-list

%type <EBinaryOp> relational-op
%type <EBinaryOp> multiplicative-op
%type <EBinaryOp> additive-op

%start head

%%

head
    : StrayWillParseQuery head-clause
;

head-clause
    : select-clause[select] from-clause[from]
        {
            head->SelectExprs = $select;
            head->FromPath = $from;
        }
    | select-clause[select] from-clause[from] where-clause[where]
        {
            head->SelectExprs = $select;
            head->WherePredicate = $where;
            head->FromPath = $from;
        }
    | select-clause[select] from-clause[from] where-clause[where] group-by-clause[group]
        {
            head->SelectExprs = $select;
            head->WherePredicate = $where;
            head->GroupExprs = $group;
            head->FromPath = $from;
        }
    | select-clause[select] from-clause[from] group-by-clause[group]
        {
            head->SelectExprs = $select;
            head->GroupExprs = $group;
            head->FromPath = $from;
        }
;

select-clause
    : named-expression-list[projections]
        {
            $$ = $projections;
        }
    | Asterisk
        {
            $$ = TNullableNamedExprs();
        }
;

from-clause
    : KwFrom Identifier[path]
        {
            $$ = $path;
        }
;

where-clause
    : KwWhere or-op-expr[predicate]
        {
            $$ = $predicate;
        }
;

group-by-clause
    : KwGroupBy named-expression-list[exprs]
        {
            $$ = $exprs;
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
    : expression[expr]
        {
            $$ = TNamedExpression($expr, InferName($expr.Get()));
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
            $$ = New<TBinaryOpExpression>(@$, EBinaryOp::Or, $lhs, $rhs);
        }
    | and-op-expr
        { $$ = $1; }
;

and-op-expr
    : and-op-expr[lhs] KwAnd relational-op-expr[rhs]
        {
            $$ = New<TBinaryOpExpression>(@$, EBinaryOp::And, $lhs, $rhs);
        }
    | relational-op-expr
        { $$ = $1; }
;

relational-op-expr
    : relational-op-expr[lhs] relational-op[opcode] additive-op-expr[rhs]
        {
            $$ = New<TBinaryOpExpression>(@$, $opcode, $lhs, $rhs);
        }
    | atomic-expr[expr] KwBetween atomic-expr[lbexpr] KwAnd atomic-expr[rbexpr]
        {
            $$ = New<TBinaryOpExpression>(@$, EBinaryOp::And,
                New<TBinaryOpExpression>(@$, EBinaryOp::GreaterOrEqual, $expr, $lbexpr),
                New<TBinaryOpExpression>(@$, EBinaryOp::LessOrEqual, $expr, $rbexpr));

        }
    | atomic-expr[expr] KwIn LeftParenthesis literal-tuple-list[args] RightParenthesis
        {
            $$ = New<TInExpression>(@$, $expr, $args);
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
            $$ = New<TBinaryOpExpression>(@$, $opcode, $lhs, $rhs);
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
            $$ = New<TBinaryOpExpression>(@$, $opcode, $lhs, $rhs);
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

comma-expr
    : comma-expr[lhs] Comma expression[rhs]
        {
            $$ = New<TCommaExpression>(@$, $lhs, $rhs);
        }
    | expression
        { $$ = $1; }
;

atomic-expr
    : Identifier[name]
        {
            $$ = New<TReferenceExpression>(@$, $name);
        }
    | Identifier[name] LeftParenthesis comma-expr[args] RightParenthesis
        {
            $$ = New<TFunctionExpression>(@$, $name, $args);
        }
    | LeftParenthesis comma-expr[expr] RightParenthesis
        {
            $$ = $expr;
        }
    | literal-expr[value]
        {
            $$ = New<TLiteralExpression>(@$, $value);
        }
;

literal-expr
    : Int64Literal
        { $$ = $1; }
    | Uint64Literal
        { $$ = $1; }
    | DoubleLiteral
        { $$ = $1; }
    | StringLiteral
        { $$ = $1; }
;

literal-list
    : literal-list[as] Comma literal-expr[a]
        {
            $$.swap($as);
            $$.push_back($a);
        }
    | literal-expr[a]
        {
            $$.push_back($a);
        }
;

literal-tuple
    : literal-expr[a]
        {
            $$.push_back($a);
        }
    | LeftParenthesis literal-list[a] RightParenthesis
        {
            $$ = $a;
        }
;

literal-tuple-list
    : literal-tuple-list[as] Comma literal-tuple[a]
        {
            $$.swap($as);
            $$.push_back($a);
        }
    | literal-tuple[a]
        {
            $$.push_back($a);
        }
;

%%

#include <core/misc/format.h>

namespace NYT {
namespace NQueryClient {
namespace NAst {

////////////////////////////////////////////////////////////////////////////////

void TParser::error(const location_type& location, const std::string& message)
{
    THROW_ERROR_EXCEPTION("Error while parsing query: %s", message.c_str())
        << TErrorAttribute("query_range", Format("%v-%v", location.first, location.second));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NAst
} // namespace NQueryClient
} // namespace NYT
