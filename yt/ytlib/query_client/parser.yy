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
%parse-param {TAstHead* head}
%parse-param {const Stroka& source}

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
%token StrayWillParseJobQuery 998
%token StrayWillParseExpression 997

// Language tokens.

%token KwFrom "keyword `FROM`"
%token KwWhere "keyword `WHERE`"
%token KwLimit "keyword `LIMIT`"
%token KwJoin "keyword `JOIN`"
%token KwUsing "keyword `USING`"
%token KwGroupBy "keyword `GROUP BY`"
%token KwOrderBy "keyword `ORDER BY`"
%token KwAs "keyword `AS`"

%token KwAnd "keyword `AND`"
%token KwOr "keyword `OR`"
%token KwNot "keyword `NOT`"
%token KwBetween "keyword `BETWEEN`"
%token KwIn "keyword `IN`"

%token KwFalse "keyword `TRUE`"
%token KwTrue "keyword `FALSE`"

%token <TStringBuf> Identifier "identifier"

%token <i64> Int64Literal "int64 literal"
%token <ui64> Uint64Literal "uint64 literal"
%token <double> DoubleLiteral "double literal"
%token <Stroka> StringLiteral "string literal"

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

%type <TNullableNamedExpressionList> select-clause-impl
%type <TExpressionPtr> where-clause-impl
%type <TNamedExpressionList> group-by-clause-impl
%type <TIdentifierList> order-by-clause-impl
%type <i64> limit-clause-impl

%type <TIdentifierList> identifier-list
%type <TNamedExpressionList> named-expression-list
%type <TNamedExpression> named-expression

%type <TExpressionPtr> expression
%type <TExpressionPtr> not-op-expr
%type <TExpressionPtr> or-op-expr
%type <TExpressionPtr> and-op-expr
%type <TExpressionPtr> relational-op-expr
%type <TExpressionPtr> multiplicative-op-expr
%type <TExpressionPtr> additive-op-expr
%type <TExpressionPtr> unary-expr
%type <TExpressionPtr> atomic-expr
%type <TExpressionPtr> comma-expr
%type <TNullable<TLiteralValue>> literal-value
%type <TLiteralValueList> literal-list
%type <TLiteralValueList> literal-tuple
%type <TLiteralValueTupleList> literal-tuple-list

%type <EUnaryOp> unary-op

%type <EBinaryOp> relational-op
%type <EBinaryOp> multiplicative-op
%type <EBinaryOp> additive-op

%start head

%%

head
    : StrayWillParseQuery parse-query
    | StrayWillParseJobQuery parse-job-query
    | StrayWillParseExpression parse-expression
;

parse-query
    : select-clause from-clause where-clause group-by-clause order-by-clause limit-clause
;

parse-job-query
    : select-clause where-clause
;

parse-expression
    : expression[expr]
        {
            head->As<TExpressionPtr>() = $expr;
        }
;

select-clause
    : select-clause-impl[select]
        {
            head->As<TQuery>().SelectExprs = $select;
        }
;

from-clause
    : KwFrom Identifier[path]
        {
            head->As<TQuery>().Source = New<TSimpleSource>(Stroka($path));
        }
    | KwFrom Identifier[left_path] KwJoin Identifier[right_path] KwUsing identifier-list[fields]
        {
            head->As<TQuery>().Source = New<TJoinSource>(Stroka($left_path), Stroka($right_path), $fields);
        }
;

where-clause
    : where-clause-impl[where]
        {
            head->As<TQuery>().WherePredicate = $where;
        }
    |
;

group-by-clause
    : group-by-clause-impl[group]
        {
            head->As<TQuery>().GroupExprs = $group;
        }
    |
;

order-by-clause
    : order-by-clause-impl[order]
        {
            head->As<TQuery>().OrderFields = $order;
        }
    |
;

limit-clause
    : limit-clause-impl[limit]
        {
            head->As<TQuery>().Limit = $limit;
        }
    |
;

select-clause-impl
    : named-expression-list[projections]
        {
            $$ = $projections;
        }
    | Asterisk
        {
            $$ = TNullableNamedExpressionList();
        }
;

where-clause-impl
    : KwWhere or-op-expr[predicate]
        {
            $$ = $predicate;
        }
;

group-by-clause-impl
    : KwGroupBy named-expression-list[exprs]
        {
            $$ = $exprs;
        }
;

order-by-clause-impl
    : KwOrderBy identifier-list[fields]
        {
            $$ = $fields;
        }
;

limit-clause-impl
    : KwLimit Int64Literal[limit]
        {
            $$ = $limit;
        }
;

identifier-list
    : identifier-list[list] Comma Identifier[value]
        {
            $$.swap($list);
            $$.push_back(Stroka($value));
        }
    | Identifier[value]
        {
            $$.push_back(Stroka($value));
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
    : and-op-expr[lhs] KwAnd not-op-expr[rhs]
        {
            $$ = New<TBinaryOpExpression>(@$, EBinaryOp::And, $lhs, $rhs);
        }
    | not-op-expr
        { $$ = $1; }
;

not-op-expr
    : KwNot relational-op-expr[expr]
        {
            $$ = New<TUnaryOpExpression>(@$, EUnaryOp::Not, $expr);
        }
    | relational-op-expr
        { $$ = $1; }
;

relational-op-expr
    : relational-op-expr[lhs] relational-op[opcode] additive-op-expr[rhs]
        {
            $$ = New<TBinaryOpExpression>(@$, $opcode, $lhs, $rhs);
        }
    | unary-expr[expr] KwBetween unary-expr[lbexpr] KwAnd unary-expr[rbexpr]
        {
            $$ = New<TBinaryOpExpression>(@$, EBinaryOp::And,
                New<TBinaryOpExpression>(@$, EBinaryOp::GreaterOrEqual, $expr, $lbexpr),
                New<TBinaryOpExpression>(@$, EBinaryOp::LessOrEqual, $expr, $rbexpr));

        }
    | unary-expr[expr] KwIn LeftParenthesis literal-tuple-list[args] RightParenthesis
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
    : multiplicative-op-expr[lhs] multiplicative-op[opcode] unary-expr[rhs]
        {
            $$ = New<TBinaryOpExpression>(@$, $opcode, $lhs, $rhs);
        }
    | unary-expr
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

unary-expr
    : unary-op[opcode] atomic-expr[rhs]
        {
            $$ = New<TUnaryOpExpression>(@$, $opcode, $rhs);
        }
    | atomic-expr
        { $$ = $1; }
;

unary-op
    : OpPlus
        { $$ = EUnaryOp::Plus; }
    | OpMinus
        { $$ = EUnaryOp::Minus; }
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
    | literal-value[value]
        {
            $$ = New<TLiteralExpression>(@$, *$value);
        }
;

literal-value
    : Int64Literal
        { $$ = $1; }
    | Uint64Literal
        { $$ = $1; }
    | DoubleLiteral
        { $$ = $1; }
    | StringLiteral
        { $$ = $1; }
    | KwFalse
        { $$ = false; }
    | KwTrue
        { $$ = true; }
;

literal-list
    : literal-list[as] Comma literal-value[a]
        {
            $$.swap($as);
            $$.push_back(*$a);
        }
    | literal-value[a]
        {
            $$.push_back(*$a);
        }
;

literal-tuple
    : literal-value[a]
        {
            $$.push_back(*$a);
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
    Stroka mark;
    for (int index = 0; index <= location.second; ++index) {
        mark += index < location.first ? ' ' : '^';
    }
    THROW_ERROR_EXCEPTION("Error while parsing query: %v", message)
        << TErrorAttribute("position", Format("%v-%v", location.first, location.second))
        << TErrorAttribute("query", Format("\n%v\n%v", source, mark));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NAst
} // namespace NQueryClient
} // namespace NYT
