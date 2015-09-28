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
        using namespace NTableClient;

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
%token KwHaving "keyword `HAVING`"
%token KwLimit "keyword `LIMIT`"
%token KwJoin "keyword `JOIN`"
%token KwUsing "keyword `USING`"
%token KwGroupBy "keyword `GROUP BY`"
%token KwOrderBy "keyword `ORDER BY`"
%token KwAsc "keyword `ASC`"
%token KwDesc "keyword `DESC`"
%token KwLeft "keyword `LEFT`"
%token KwAs "keyword `AS`"
%token KwOn "keyword `ON`"

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



%token OpTilde 33 "`~`"
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

%type <TTableDescriptor> table-descriptor

%type <bool> is-desc
%type <bool> is-left

%type <TReferenceExpressionPtr> qualified-identifier
%type <TIdentifierList> identifier-list
%type <TExpressionList> named-expression

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
%type <TNullable<TLiteralValue>> literal-value
%type <TNullable<TLiteralValue>> const-value
%type <TLiteralValueList> const-list
%type <TLiteralValueList> const-tuple
%type <TLiteralValueTupleList> const-tuple-list

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
    : select-clause from-clause where-clause group-by-clause having-clause order-by-clause limit-clause
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
            head->first.As<TExpressionPtr>() = $expr.front();
        }
;

select-clause
    : comma-expr[projections]
        {
            head->first.As<TQuery>().SelectExprs = $projections;
        }
    | Asterisk
        { }
;

table-descriptor
    : Identifier[path] Identifier[alias]
        {
            $$ = TTableDescriptor(Stroka($path), Stroka($alias));
        }
    | Identifier[path] KwAs Identifier[alias]
        {
            $$ = TTableDescriptor(Stroka($path), Stroka($alias));
        }
    |   Identifier[path]
        {
            $$ = TTableDescriptor(Stroka($path), Stroka());
        }
;

from-clause
    : KwFrom table-descriptor[table] join-clause
        {
            head->first.As<TQuery>().Table = $table;
        }
;

join-clause
    : join-clause is-left[isLeft] KwJoin table-descriptor[table] KwUsing identifier-list[fields]
        {
            head->first.As<TQuery>().Joins.emplace_back($isLeft, $table, $fields);
        }
    | join-clause is-left[isLeft] KwJoin table-descriptor[table] KwOn bitor-op-expr[lhs] OpEqual bitor-op-expr[rhs]
        {
            head->first.As<TQuery>().Joins.emplace_back($isLeft, $table, $lhs, $rhs);
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
            head->first.As<TQuery>().WherePredicate = $predicate;
        }
    |
;

group-by-clause
    : KwGroupBy comma-expr[exprs]
        {
            head->first.As<TQuery>().GroupExprs = $exprs;
        }
    |
;

having-clause
    : KwHaving or-op-expr[predicate]
        {
            head->first.As<TQuery>().HavingPredicate = $predicate;
        }
    |
;

order-by-clause
    : KwOrderBy identifier-list[fields] is-desc[isDesc]
        {
            head->first.As<TQuery>().OrderFields = $fields;
            head->first.As<TQuery>().IsDescendingOrder = $isDesc;
        }
    |
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
            head->first.As<TQuery>().Limit = $limit;
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

named-expression
    : expression[expr]
        {
            $$ = $expr;
        }
    | expression[expr] KwAs Identifier[name]
        {
            if ($expr.size() != 1) {
                THROW_ERROR_EXCEPTION("Aliased expression %Qv must be scalar", GetSource(@$, source));
            }
            auto inserted = head->second.insert(MakePair(Stroka($name), $expr.front()));
            if (!inserted.second) {
                THROW_ERROR_EXCEPTION("Alias %Qv has been already used", $name);
            }
            $$ = MakeExpr<TReferenceExpression>(@$, $name);
        }
;

expression
    : or-op-expr
        { $$ = $1; }
;

or-op-expr
    : or-op-expr[lhs] KwOr and-op-expr[rhs]
        {
            $$ = MakeExpr<TBinaryOpExpression>(@$, EBinaryOp::Or, $lhs, $rhs);
        }
    | and-op-expr
        { $$ = $1; }
;

and-op-expr

    : and-op-expr[lhs] KwAnd not-op-expr[rhs]
        {
            $$ = MakeExpr<TBinaryOpExpression>(@$, EBinaryOp::And, $lhs, $rhs);
        }
    | not-op-expr
        { $$ = $1; }
;

not-op-expr
    : KwNot equal-op-expr[expr]
        {
            $$ = MakeExpr<TUnaryOpExpression>(@$, EUnaryOp::Not, $expr);
        }
    | equal-op-expr
        { $$ = $1; }
;

equal-op-expr
    : equal-op-expr[lhs] OpEqual relational-op-expr[rhs]
        {
            $$ = MakeExpr<TBinaryOpExpression>(@$, EBinaryOp::Equal, $lhs, $rhs);
        }

    | equal-op-expr[lhs] OpNotEqual relational-op-expr[rhs]
        {
            $$ = MakeExpr<TBinaryOpExpression>(@$, EBinaryOp::NotEqual, $lhs, $rhs);
        }
    | relational-op-expr
        { $$ = $1; }
;

relational-op-expr
    : relational-op-expr[lhs] relational-op[opcode] bitor-op-expr[rhs]
        {
            $$ = MakeExpr<TBinaryOpExpression>(@$, $opcode, $lhs, $rhs);
        }
    | unary-expr[expr] KwBetween unary-expr[lbexpr] KwAnd unary-expr[rbexpr]
        {
            $$ = MakeExpr<TBinaryOpExpression>(@$, EBinaryOp::And,
                MakeExpr<TBinaryOpExpression>(@$, EBinaryOp::GreaterOrEqual, $expr, $lbexpr),
                MakeExpr<TBinaryOpExpression>(@$, EBinaryOp::LessOrEqual, $expr, $rbexpr));

        }
    | unary-expr[expr] KwIn LeftParenthesis const-tuple-list[args] RightParenthesis
        {
            $$ = MakeExpr<TInExpression>(@$, $expr, $args);
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
    : shift-op-expr[lhs] OpVerticalBar bitand-op-expr[rhs]
        {
            $$ = MakeExpr<TBinaryOpExpression>(@$, EBinaryOp::BitOr, $lhs, $rhs);
        }
    | bitand-op-expr
        { $$ = $1; }
;

bitand-op-expr
    : shift-op-expr[lhs] OpAmpersand shift-op-expr[rhs]
        {
            $$ = MakeExpr<TBinaryOpExpression>(@$, EBinaryOp::BitAnd, $lhs, $rhs);
        }
    | shift-op-expr
        { $$ = $1; }
;

shift-op-expr
    : shift-op-expr[lhs] OpLeftShift additive-op-expr[rhs]
        {
            $$ = MakeExpr<TBinaryOpExpression>(@$, EBinaryOp::LeftShift, $lhs, $rhs);
        }
    | shift-op-expr[lhs] OpRightShift additive-op-expr[rhs]
        {
            $$ = MakeExpr<TBinaryOpExpression>(@$, EBinaryOp::RightShift, $lhs, $rhs);
        }
    | additive-op-expr
        { $$ = $1; }
;

additive-op-expr
    : additive-op-expr[lhs] additive-op[opcode] multiplicative-op-expr[rhs]
        {
            $$ = MakeExpr<TBinaryOpExpression>(@$, $opcode, $lhs, $rhs);
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
            $$ = MakeExpr<TBinaryOpExpression>(@$, $opcode, $lhs, $rhs);
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
    : comma-expr[lhs] Comma named-expression[rhs]
        {
            $$ = $lhs;
            $$.insert($$.end(), $rhs.begin(), $rhs.end());
        }
    | named-expression
        { $$ = $1; }
;

unary-expr
    : unary-op[opcode] atomic-expr[rhs]
        {
            $$ = MakeExpr<TUnaryOpExpression>(@$, $opcode, $rhs);
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
            $$ = New<TReferenceExpression>(@$, $name);
        }
    | Identifier[table] Dot Identifier[name]
        {
            $$ = New<TReferenceExpression>(@$, $name, $table);
        }
;

atomic-expr
    : qualified-identifier[identifier]
        {
            $$ = TExpressionList(1, $identifier);
        }
    | Identifier[name] LeftParenthesis RightParenthesis
        {
            $$ = MakeExpr<TFunctionExpression>(@$, $name, TExpressionList());
        }
    | Identifier[name] LeftParenthesis comma-expr[args] RightParenthesis
        {
            $$ = MakeExpr<TFunctionExpression>(@$, $name, $args);
        }
    | LeftParenthesis comma-expr[expr] RightParenthesis
        {
            $$ = $expr;
        }
    | literal-value[value]
        {
            $$ = MakeExpr<TLiteralExpression>(@$, *$value);
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

const-value
    : unary-op[op] literal-value[value]
        {
            switch ($op) {
                case EUnaryOp::Minus: {
                    if (auto data = $value->TryAs<i64>()) {
                        $$ = i64(0) - *data;
                    } else if (auto data = $value->TryAs<ui64>()) {
                        $$ = ui64(0) - *data;
                    } else if (auto data = $value->TryAs<double>()) {
                        $$ = double(0) - *data;
                    } else {
                        THROW_ERROR_EXCEPTION("Negation of unsupported type");
                    }
                    break;
                }
                case EUnaryOp::Plus:
                    $$ = $value;
                    break;
                default:
                    YUNREACHABLE();
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
