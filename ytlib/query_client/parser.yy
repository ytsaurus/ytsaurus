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
%parse-param {const TString& source}

%code requires {
    #include "ast.h"

    namespace NYT { namespace NQueryClient { namespace NAst {
        using namespace NTableClient;

        class TLexer;
        class TParser;
    } } }
}

%code {
    #include <yt/ytlib/query_client/lexer.h>

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
%token KwHaving "keyword `HAVING`"
%token KwLimit "keyword `LIMIT`"
%token KwJoin "keyword `JOIN`"
%token KwUsing "keyword `USING`"
%token KwGroupBy "keyword `GROUP BY`"
%token KwWithTotals "keyword `WITH TOTALS`"
%token KwOrderBy "keyword `ORDER BY`"
%token KwAsc "keyword `ASC`"
%token KwDesc "keyword `DESC`"
%token KwLeft "keyword `LEFT`"
%token KwAs "keyword `AS`"
%token KwOn "keyword `ON`"

%token KwAnd "keyword `AND`"
%token KwOr "keyword `OR`"
%token KwNot "keyword `NOT`"
%token KwNull "keyword `NULL`"
%token KwBetween "keyword `BETWEEN`"
%token KwIn "keyword `IN`"
%token KwTransform "keyword `TRANSFORM`"

%token KwFalse "keyword `TRUE`"
%token KwTrue "keyword `FALSE`"

%token <TStringBuf> Identifier "identifier"

%token <i64> Int64Literal "int64 literal"
%token <ui64> Uint64Literal "uint64 literal"
%token <double> DoubleLiteral "double literal"
%token <TString> StringLiteral "string literal"



%token OpTilde 126 "`~`"
%token OpNumberSign 35 "`#`"
%token OpVerticalBar 124 "`|`"
%token OpAmpersand 38 "`&`"
%token OpModulo 37 "`%`"
%token OpLeftShift "`<<`"
%token OpRightShift "`>>`"

%token LeftParenthesis 40 "`(`"
%token RightParenthesis 41 "`)`"

%token Asterisk 42 "`*`"
%token OpPlus 43 "`+`"
%token Comma 44 "`,`"
%token OpMinus 45 "`-`"
%token Dot 46 "`.`"
%token OpDivide 47 "`/`"


%token OpLess 60 "`<`"
%token OpLessOrEqual "`<=`"
%token OpEqual 61 "`=`"
%token OpNotEqual "`!=`"
%token OpGreater 62 "`>`"
%token OpGreaterOrEqual "`>=`"

%type <ETotalsMode> group-by-clause-tail

%type <TTableDescriptor> table-descriptor

%type <bool> is-desc
%type <bool> is-left

%type <TReferenceExpressionPtr> qualified-identifier
%type <TIdentifierList> identifier-list

%type <TOrderExpressionList> order-expr-list
%type <TExpressionList> expression
%type <TExpressionList> or-op-expr
%type <TExpressionList> and-op-expr
%type <TExpressionList> not-op-expr
%type <TExpressionList> equal-op-expr
%type <TExpressionList> relational-op-expr
%type <TExpressionList> bitor-op-expr
%type <TExpressionList> bitand-op-expr
%type <TExpressionList> shift-op-expr
%type <TExpressionList> multiplicative-op-expr
%type <TExpressionList> additive-op-expr
%type <TExpressionList> unary-expr
%type <TExpressionList> atomic-expr
%type <TExpressionList> comma-expr
%type <TNullableExpressionList> transform-default-expr
%type <TNullableExpressionList> join-predicate

%type <TNullable<TLiteralValue>> literal-value
%type <TNullable<TLiteralValue>> const-value
%type <TLiteralValueList> const-list
%type <TLiteralValueList> const-tuple
%type <TLiteralValueTupleList> const-tuple-list
%type <TLiteralValueRangeList> const-range-list

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
            if ($expr.size() != 1) {
                THROW_ERROR_EXCEPTION("Expected scalar expression, got %Qv", GetSource(@$, source));
            }
            head->Ast.As<TExpressionPtr>() = $expr.front();
        }
;

select-clause
    : comma-expr[projections]
        {
            head->Ast.As<TQuery>().SelectExprs = $projections;
        }
    | Asterisk
        { }
;

table-descriptor
    : Identifier[path] Identifier[alias]
        {
            $$ = TTableDescriptor(TString($path), TString($alias));
        }
    | Identifier[path] KwAs Identifier[alias]
        {
            $$ = TTableDescriptor(TString($path), TString($alias));
        }
    |   Identifier[path]
        {
            $$ = TTableDescriptor(TString($path));
        }
;

from-clause
    : KwFrom table-descriptor[table] join-clause
        {
            head->Ast.As<TQuery>().Table = $table;
        }
;

join-predicate
    : KwAnd and-op-expr[predicate]
        {
            $$ = $predicate;
        }
    | { }
;

join-clause
    : join-clause is-left[isLeft] KwJoin table-descriptor[table] KwUsing identifier-list[fields] join-predicate[predicate]
        {
            head->Ast.As<TQuery>().Joins.emplace_back($isLeft, $table, $fields, $predicate);
        }
    | join-clause is-left[isLeft] KwJoin table-descriptor[table] KwOn bitor-op-expr[lhs] OpEqual bitor-op-expr[rhs] join-predicate[predicate]
        {
            head->Ast.As<TQuery>().Joins.emplace_back($isLeft, $table, $lhs, $rhs, $predicate);
        }
    |
;

is-left
    : KwLeft
        {
            $$ = true;
        }
    |
        {
            $$ = false;
        }
;

where-clause
    : KwWhere or-op-expr[predicate]
        {
            head->Ast.As<TQuery>().WherePredicate = $predicate;
        }
    |
;

group-by-clause
    : KwGroupBy comma-expr[exprs] group-by-clause-tail[totalsMode]
        {
            head->Ast.As<TQuery>().GroupExprs = std::make_pair($exprs, $totalsMode);
        }
    |
;

group-by-clause-tail
    : KwWithTotals
        {
            $$ = ETotalsMode::BeforeHaving;
        }
    | having-clause
        {
            $$ = ETotalsMode::None;
        }
    | having-clause KwWithTotals
        {
            $$ = ETotalsMode::AfterHaving;
        }
    | KwWithTotals having-clause
        {
            $$ = ETotalsMode::BeforeHaving;
        }
    |
        {
            $$ = ETotalsMode::None;
        }
;

having-clause
    : KwHaving or-op-expr[predicate]
        {
            head->Ast.As<TQuery>().HavingPredicate = $predicate;
        }
;

order-by-clause
    : KwOrderBy order-expr-list[exprs]
        {
            head->Ast.As<TQuery>().OrderExpressions = $exprs;
        }
    |
;

order-expr-list
    : order-expr-list[list] Comma expression[expr] is-desc[isDesc]
        {
            $$.swap($list);
            $$.emplace_back($expr, $isDesc);
        }
    | expression[expr] is-desc[isDesc]
        {
            $$.emplace_back($expr, $isDesc);
        }
;

is-desc
    : KwDesc
        {
            $$ = true;
        }
    | KwAsc
        {
            $$ = false;
        }
    |
        {
            $$ = false;
        }
;

limit-clause
    : KwLimit Int64Literal[limit]
        {
            head->Ast.As<TQuery>().Limit = $limit;
        }
    |
;

identifier-list
    : identifier-list[list] Comma qualified-identifier[value]
        {
            $$.swap($list);
            $$.push_back($value);
        }
    | qualified-identifier[value]
        {
            $$.push_back($value);
        }
;

expression
    : or-op-expr
        { $$ = $1; }
    | or-op-expr[expr] KwAs Identifier[name]
        {
            if ($expr.size() != 1) {
                THROW_ERROR_EXCEPTION("Aliased expression %Qv must be scalar", GetSource(@$, source));
            }
            auto inserted = head->AliasMap.insert(std::make_pair(TString($name), $expr.front())).second;
            if (!inserted) {
                THROW_ERROR_EXCEPTION("Alias %Qv has been already used", $name);
            }
            $$ = MakeExpression<TAliasExpression>(@$, $expr.front(), $name);
        }
;

or-op-expr
    : or-op-expr[lhs] KwOr and-op-expr[rhs]
        {
            $$ = MakeExpression<TBinaryOpExpression>(@$, EBinaryOp::Or, $lhs, $rhs);
        }
    | and-op-expr
        { $$ = $1; }
;

and-op-expr

    : and-op-expr[lhs] KwAnd not-op-expr[rhs]
        {
            $$ = MakeExpression<TBinaryOpExpression>(@$, EBinaryOp::And, $lhs, $rhs);
        }
    | not-op-expr
        { $$ = $1; }
;

not-op-expr
    : KwNot equal-op-expr[expr]
        {
            $$ = MakeExpression<TUnaryOpExpression>(@$, EUnaryOp::Not, $expr);
        }
    | equal-op-expr
        { $$ = $1; }
;

equal-op-expr
    : equal-op-expr[lhs] OpEqual relational-op-expr[rhs]
        {
            $$ = MakeExpression<TBinaryOpExpression>(@$, EBinaryOp::Equal, $lhs, $rhs);
        }

    | equal-op-expr[lhs] OpNotEqual relational-op-expr[rhs]
        {
            $$ = MakeExpression<TBinaryOpExpression>(@$, EBinaryOp::NotEqual, $lhs, $rhs);
        }
    | relational-op-expr
        { $$ = $1; }
;

relational-op-expr
    : relational-op-expr[lhs] relational-op[opcode] bitor-op-expr[rhs]
        {
            $$ = MakeExpression<TBinaryOpExpression>(@$, $opcode, $lhs, $rhs);
        }
    | unary-expr[expr] KwBetween const-tuple[lower] KwAnd const-tuple[upper]
        {
            TExpressionList lowerExpr;
            for (const auto& value : $lower) {
                lowerExpr.push_back(New<TLiteralExpression>(@$, value));
            }

            TExpressionList upperExpr;
            for (const auto& value : $upper) {
                upperExpr.push_back(New<TLiteralExpression>(@$, value));
            }

            $$ = MakeExpression<TBinaryOpExpression>(@$, EBinaryOp::And,
                MakeExpression<TBinaryOpExpression>(@$, EBinaryOp::GreaterOrEqual, $expr, lowerExpr),
                MakeExpression<TBinaryOpExpression>(@$, EBinaryOp::LessOrEqual, $expr, upperExpr));
        }
    | unary-expr[expr] KwBetween LeftParenthesis const-range-list[ranges] RightParenthesis
        {
            $$ = MakeExpression<TBetweenExpression>(@$, $expr, $ranges);
        }
    | unary-expr[expr] KwIn LeftParenthesis const-tuple-list[args] RightParenthesis
        {
            $$ = MakeExpression<TInExpression>(@$, $expr, $args);
        }
    | bitor-op-expr
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

bitor-op-expr
    : bitor-op-expr[lhs] OpVerticalBar bitand-op-expr[rhs]
        {
            $$ = MakeExpression<TBinaryOpExpression>(@$, EBinaryOp::BitOr, $lhs, $rhs);
        }
    | bitand-op-expr
        { $$ = $1; }
;

bitand-op-expr
    : bitand-op-expr[lhs] OpAmpersand shift-op-expr[rhs]
        {
            $$ = MakeExpression<TBinaryOpExpression>(@$, EBinaryOp::BitAnd, $lhs, $rhs);
        }
    | shift-op-expr
        { $$ = $1; }
;

shift-op-expr
    : shift-op-expr[lhs] OpLeftShift additive-op-expr[rhs]
        {
            $$ = MakeExpression<TBinaryOpExpression>(@$, EBinaryOp::LeftShift, $lhs, $rhs);
        }
    | shift-op-expr[lhs] OpRightShift additive-op-expr[rhs]
        {
            $$ = MakeExpression<TBinaryOpExpression>(@$, EBinaryOp::RightShift, $lhs, $rhs);
        }
    | additive-op-expr
        { $$ = $1; }
;

additive-op-expr
    : additive-op-expr[lhs] additive-op[opcode] multiplicative-op-expr[rhs]
        {
            $$ = MakeExpression<TBinaryOpExpression>(@$, $opcode, $lhs, $rhs);
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
            $$ = MakeExpression<TBinaryOpExpression>(@$, $opcode, $lhs, $rhs);
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
            $$ = $lhs;
            $$.insert($$.end(), $rhs.begin(), $rhs.end());
        }
    | expression
        { $$ = $1; }
;

unary-expr
    : unary-op[opcode] atomic-expr[rhs]
        {
            $$ = MakeExpression<TUnaryOpExpression>(@$, $opcode, $rhs);
        }
    | atomic-expr
        { $$ = $1; }
;

unary-op
    : OpPlus
        { $$ = EUnaryOp::Plus; }
    | OpMinus
        { $$ = EUnaryOp::Minus; }
    | OpTilde
        { $$ = EUnaryOp::BitNot; }
;

qualified-identifier
    : Identifier[name]
        {
            $$ = New<TReferenceExpression>(@$, TString($name));
        }
    | Identifier[table] Dot Identifier[name]
        {
            $$ = New<TReferenceExpression>(@$, TString($name), TString($table));
        }
;

atomic-expr
    : qualified-identifier[identifier]
        {
            $$ = TExpressionList(1, $identifier);
        }
    | Identifier[name] LeftParenthesis RightParenthesis
        {
            $$ = MakeExpression<TFunctionExpression>(@$, $name, TExpressionList());
        }
    | Identifier[name] LeftParenthesis comma-expr[args] RightParenthesis
        {
            $$ = MakeExpression<TFunctionExpression>(@$, $name, $args);
        }
    | KwTransform LeftParenthesis expression[expr] Comma LeftParenthesis const-tuple-list[from] RightParenthesis Comma LeftParenthesis const-tuple-list[to] RightParenthesis transform-default-expr[default] RightParenthesis
        {
            $$ = MakeExpression<TTransformExpression>(@$, $expr, $from, $to, $default);
        }
    | LeftParenthesis comma-expr[expr] RightParenthesis
        {
            $$ = $expr;
        }
    | literal-value[value]
        {
            $$ = MakeExpression<TLiteralExpression>(@$, *$value);
        }
;

transform-default-expr
    : Comma expression[expr]
        {
            $$ = $expr;
        }
    | { }
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
    | KwNull
        { $$ = TNullLiteralValue(); }
    | OpNumberSign
        { $$ = TNullLiteralValue(); }
;

const-value
    : unary-op[op] literal-value[value]
        {
            switch ($op) {
                case EUnaryOp::Minus: {
                    if (auto data = $value->TryAs<i64>()) {
                        $$ = -*data;
                    } else if (auto data = $value->TryAs<ui64>()) {
                        $$ = -*data;
                    } else if (auto data = $value->TryAs<double>()) {
                        $$ = -*data;
                    } else {
                        THROW_ERROR_EXCEPTION("Negation of unsupported type");
                    }
                    break;
                }
                case EUnaryOp::Plus:
                    $$ = $value;
                    break;
                case EUnaryOp::BitNot: {
                    if (auto data = $value->TryAs<i64>()) {
                        $$ = ~*data;
                    } else if (auto data = $value->TryAs<ui64>()) {
                        $$ = ~*data;
                    } else {
                        THROW_ERROR_EXCEPTION("Bitwise negation of unsupported type");
                    }
                    break;
                }
                default:
                    Y_UNREACHABLE();
            }

        }
    | literal-value[value]
        { $$ = $value; }
;

const-list
    : const-list[as] Comma const-value[a]
        {
            $$.swap($as);
            $$.push_back(*$a);
        }
    | const-value[a]
        {
            $$.push_back(*$a);
        }
;

const-tuple
    : const-value[a]
        {
            $$.push_back(*$a);
        }
    | LeftParenthesis const-list[a] RightParenthesis
        {
            $$ = $a;
        }
;

const-tuple-list
    : const-tuple-list[as] Comma const-tuple[a]
        {
            $$.swap($as);
            $$.push_back($a);
        }
    | const-tuple[a]
        {
            $$.push_back($a);
        }
;

const-range-list
    : const-range-list[as] Comma const-tuple[a] KwAnd const-tuple[b]
        {
            $$.swap($as);
            $$.emplace_back($a, $b);
        }
    | const-tuple[a] KwAnd const-tuple[b]
        {
            $$.emplace_back($a, $b);
        }
;

%%

#include <yt/core/misc/format.h>

namespace NYT {
namespace NQueryClient {
namespace NAst {

////////////////////////////////////////////////////////////////////////////////

void TParser::error(const location_type& location, const std::string& message)
{
    auto leftContextStart = std::max<size_t>(location.first, 16) - 16;
    auto rightContextEnd = std::min<size_t>(location.second + 16, source.size());

    THROW_ERROR_EXCEPTION("Error while parsing query: %v", message)
        << TErrorAttribute("position", Format("%v-%v", location.first, location.second))
        << TErrorAttribute("query", Format("%v >>>>> %v <<<<< %v",
            source.substr(leftContextStart, location.first - leftContextStart),
            source.substr(location.first, location.second - location.first),
            source.substr(location.second, rightContextEnd - location.second)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NAst
} // namespace NQueryClient
} // namespace NYT
