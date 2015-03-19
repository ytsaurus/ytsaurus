// A Bison parser, made by GNU Bison 3.0.

// Skeleton implementation for Bison LALR(1) parsers in C++

// Copyright (C) 2002-2013 Free Software Foundation, Inc.

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

// As a special exception, you may create a larger work that contains
// part or all of the Bison parser skeleton and distribute that work
// under terms of your choice, so long as that work isn't itself a
// parser generator using the skeleton or a modified version thereof
// as a parser skeleton.  Alternatively, if you modify or redistribute
// the parser skeleton itself, you may (at your option) remove this
// special exception, which will cause the skeleton and the resulting
// Bison output files to be licensed under the GNU General Public
// License without this special exception.

// This special exception was added by the Free Software Foundation in
// version 2.2 of Bison.

// Take the name prefix into account.
#define yylex   yt_ql_yylex

// First part of user declarations.


# ifndef YY_NULL
#  if defined __cplusplus && 201103L <= __cplusplus
#   define YY_NULL nullptr
#  else
#   define YY_NULL 0
#  endif
# endif

#include "parser.hpp"

// User implementation prologue.

// Unqualified %code blocks.

    #include <ytlib/query_client/lexer.h>
    #include <ytlib/new_table_client/row_buffer.h>

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



#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> // FIXME: INFRINGES ON USER NAME SPACE.
#   define YY_(msgid) dgettext ("bison-runtime", msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(msgid) msgid
# endif
#endif

#define YYRHSLOC(Rhs, K) ((Rhs)[K].location)
/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

# ifndef YYLLOC_DEFAULT
#  define YYLLOC_DEFAULT(Current, Rhs, N)                               \
    do                                                                  \
      if (N)                                                            \
        {                                                               \
          (Current).begin  = YYRHSLOC (Rhs, 1).begin;                   \
          (Current).end    = YYRHSLOC (Rhs, N).end;                     \
        }                                                               \
      else                                                              \
        {                                                               \
          (Current).begin = (Current).end = YYRHSLOC (Rhs, 0).end;      \
        }                                                               \
    while (/*CONSTCOND*/ false)
# endif


// Suppress unused-variable warnings by "using" E.
#define YYUSE(E) ((void) (E))

// Enable debugging if requested.
#if YT_QL_YYDEBUG

// A pseudo ostream that takes yydebug_ into account.
# define YYCDEBUG if (yydebug_) (*yycdebug_)

# define YY_SYMBOL_PRINT(Title, Symbol)         \
  do {                                          \
    if (yydebug_)                               \
    {                                           \
      *yycdebug_ << Title << ' ';               \
      yy_print_ (*yycdebug_, Symbol);           \
      *yycdebug_ << std::endl;                  \
    }                                           \
  } while (false)

# define YY_REDUCE_PRINT(Rule)          \
  do {                                  \
    if (yydebug_)                       \
      yy_reduce_print_ (Rule);          \
  } while (false)

# define YY_STACK_PRINT()               \
  do {                                  \
    if (yydebug_)                       \
      yystack_print_ ();                \
  } while (false)

#else // !YT_QL_YYDEBUG

# define YYCDEBUG if (false) std::cerr
# define YY_SYMBOL_PRINT(Title, Symbol)  YYUSE(Symbol)
# define YY_REDUCE_PRINT(Rule)           static_cast<void>(0)
# define YY_STACK_PRINT()                static_cast<void>(0)

#endif // !YT_QL_YYDEBUG

#define yyerrok         (yyerrstatus_ = 0)
#define yyclearin       (yyempty = true)

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYRECOVERING()  (!!yyerrstatus_)

namespace NYT { namespace NQueryClient { namespace NAst {

  /* Return YYSTR after stripping away unnecessary quotes and
     backslashes, so that it's suitable for yyerror.  The heuristic is
     that double-quoting is unnecessary unless the string contains an
     apostrophe, a comma, or backslash (other than backslash-backslash).
     YYSTR is taken from yytname.  */
  std::string
  TParser::yytnamerr_ (const char *yystr)
  {
    if (*yystr == '"')
      {
        std::string yyr = "";
        char const *yyp = yystr;

        for (;;)
          switch (*++yyp)
            {
            case '\'':
            case ',':
              goto do_not_strip_quotes;

            case '\\':
              if (*++yyp != '\\')
                goto do_not_strip_quotes;
              // Fall through.
            default:
              yyr += *yyp;
              break;

            case '"':
              return yyr;
            }
      do_not_strip_quotes: ;
      }

    return yystr;
  }


  /// Build a parser object.
  TParser::TParser (TLexer& lexer_yyarg, TAstHead* head_yyarg, TRowBuffer* rowBuffer_yyarg, const Stroka& source_yyarg)
    :
#if YT_QL_YYDEBUG
      yydebug_ (false),
      yycdebug_ (&std::cerr),
#endif
      lexer (lexer_yyarg),
      head (head_yyarg),
      rowBuffer (rowBuffer_yyarg),
      source (source_yyarg)
  {}

  TParser::~TParser ()
  {}


  /*---------------.
  | Symbol types.  |
  `---------------*/

  inline
  TParser::syntax_error::syntax_error (const location_type& l, const std::string& m)
    : std::runtime_error (m)
    , location (l)
  {}

  // basic_symbol.
  template <typename Base>
  inline
  TParser::basic_symbol<Base>::basic_symbol ()
    : value ()
  {}

  template <typename Base>
  inline
  TParser::basic_symbol<Base>::basic_symbol (const basic_symbol& other)
    : Base (other)
    , value ()
    , location (other.location)
  {
      switch (other.type_get ())
    {
      case 59: // relational-op
      case 61: // additive-op
      case 63: // multiplicative-op
        value.copy< EBinaryOp > (other.value);
        break;

      case 66: // unary-op
        value.copy< EUnaryOp > (other.value);
        break;

      case 23: // "string literal"
        value.copy< Stroka > (other.value);
        break;

      case 49: // where-clause
      case 55: // expression
      case 56: // or-op-expr
      case 57: // and-op-expr
      case 58: // relational-op-expr
      case 60: // additive-op-expr
      case 62: // multiplicative-op-expr
      case 64: // comma-expr
      case 65: // unary-expr
      case 67: // atomic-expr
        value.copy< TExpressionPtr > (other.value);
        break;

      case 48: // identifier-list
      case 51: // order-by-clause
        value.copy< TIdentifierList > (other.value);
        break;

      case 54: // named-expression
        value.copy< TNamedExpression > (other.value);
        break;

      case 50: // group-by-clause
      case 53: // named-expression-list
        value.copy< TNamedExpressionList > (other.value);
        break;

      case 41: // select-clause
        value.copy< TNullableNamedExprs > (other.value);
        break;

      case 19: // "identifier"
        value.copy< TStringBuf > (other.value);
        break;

      case 68: // literal-expr
        value.copy< TUnversionedValue > (other.value);
        break;

      case 69: // literal-list
      case 70: // literal-tuple
        value.copy< TValueList > (other.value);
        break;

      case 71: // literal-tuple-list
        value.copy< TValueTupleList > (other.value);
        break;

      case 22: // "double literal"
        value.copy< double > (other.value);
        break;

      case 20: // "int64 literal"
      case 52: // limit-clause
        value.copy< i64 > (other.value);
        break;

      case 21: // "uint64 literal"
        value.copy< ui64 > (other.value);
        break;

      default:
        break;
    }

  }


  template <typename Base>
  inline
  TParser::basic_symbol<Base>::basic_symbol (typename Base::kind_type t, const semantic_type& v, const location_type& l)
    : Base (t)
    , value ()
    , location (l)
  {
    (void) v;
      switch (this->type_get ())
    {
      case 59: // relational-op
      case 61: // additive-op
      case 63: // multiplicative-op
        value.copy< EBinaryOp > (v);
        break;

      case 66: // unary-op
        value.copy< EUnaryOp > (v);
        break;

      case 23: // "string literal"
        value.copy< Stroka > (v);
        break;

      case 49: // where-clause
      case 55: // expression
      case 56: // or-op-expr
      case 57: // and-op-expr
      case 58: // relational-op-expr
      case 60: // additive-op-expr
      case 62: // multiplicative-op-expr
      case 64: // comma-expr
      case 65: // unary-expr
      case 67: // atomic-expr
        value.copy< TExpressionPtr > (v);
        break;

      case 48: // identifier-list
      case 51: // order-by-clause
        value.copy< TIdentifierList > (v);
        break;

      case 54: // named-expression
        value.copy< TNamedExpression > (v);
        break;

      case 50: // group-by-clause
      case 53: // named-expression-list
        value.copy< TNamedExpressionList > (v);
        break;

      case 41: // select-clause
        value.copy< TNullableNamedExprs > (v);
        break;

      case 19: // "identifier"
        value.copy< TStringBuf > (v);
        break;

      case 68: // literal-expr
        value.copy< TUnversionedValue > (v);
        break;

      case 69: // literal-list
      case 70: // literal-tuple
        value.copy< TValueList > (v);
        break;

      case 71: // literal-tuple-list
        value.copy< TValueTupleList > (v);
        break;

      case 22: // "double literal"
        value.copy< double > (v);
        break;

      case 20: // "int64 literal"
      case 52: // limit-clause
        value.copy< i64 > (v);
        break;

      case 21: // "uint64 literal"
        value.copy< ui64 > (v);
        break;

      default:
        break;
    }
}


  // Implementation of basic_symbol constructor for each type.

  template <typename Base>
  TParser::basic_symbol<Base>::basic_symbol (typename Base::kind_type t, const location_type& l)
    : Base (t)
    , value ()
    , location (l)
  {}

  template <typename Base>
  TParser::basic_symbol<Base>::basic_symbol (typename Base::kind_type t, const EBinaryOp v, const location_type& l)
    : Base (t)
    , value (v)
    , location (l)
  {}

  template <typename Base>
  TParser::basic_symbol<Base>::basic_symbol (typename Base::kind_type t, const EUnaryOp v, const location_type& l)
    : Base (t)
    , value (v)
    , location (l)
  {}

  template <typename Base>
  TParser::basic_symbol<Base>::basic_symbol (typename Base::kind_type t, const Stroka v, const location_type& l)
    : Base (t)
    , value (v)
    , location (l)
  {}

  template <typename Base>
  TParser::basic_symbol<Base>::basic_symbol (typename Base::kind_type t, const TExpressionPtr v, const location_type& l)
    : Base (t)
    , value (v)
    , location (l)
  {}

  template <typename Base>
  TParser::basic_symbol<Base>::basic_symbol (typename Base::kind_type t, const TIdentifierList v, const location_type& l)
    : Base (t)
    , value (v)
    , location (l)
  {}

  template <typename Base>
  TParser::basic_symbol<Base>::basic_symbol (typename Base::kind_type t, const TNamedExpression v, const location_type& l)
    : Base (t)
    , value (v)
    , location (l)
  {}

  template <typename Base>
  TParser::basic_symbol<Base>::basic_symbol (typename Base::kind_type t, const TNamedExpressionList v, const location_type& l)
    : Base (t)
    , value (v)
    , location (l)
  {}

  template <typename Base>
  TParser::basic_symbol<Base>::basic_symbol (typename Base::kind_type t, const TNullableNamedExprs v, const location_type& l)
    : Base (t)
    , value (v)
    , location (l)
  {}

  template <typename Base>
  TParser::basic_symbol<Base>::basic_symbol (typename Base::kind_type t, const TStringBuf v, const location_type& l)
    : Base (t)
    , value (v)
    , location (l)
  {}

  template <typename Base>
  TParser::basic_symbol<Base>::basic_symbol (typename Base::kind_type t, const TUnversionedValue v, const location_type& l)
    : Base (t)
    , value (v)
    , location (l)
  {}

  template <typename Base>
  TParser::basic_symbol<Base>::basic_symbol (typename Base::kind_type t, const TValueList v, const location_type& l)
    : Base (t)
    , value (v)
    , location (l)
  {}

  template <typename Base>
  TParser::basic_symbol<Base>::basic_symbol (typename Base::kind_type t, const TValueTupleList v, const location_type& l)
    : Base (t)
    , value (v)
    , location (l)
  {}

  template <typename Base>
  TParser::basic_symbol<Base>::basic_symbol (typename Base::kind_type t, const double v, const location_type& l)
    : Base (t)
    , value (v)
    , location (l)
  {}

  template <typename Base>
  TParser::basic_symbol<Base>::basic_symbol (typename Base::kind_type t, const i64 v, const location_type& l)
    : Base (t)
    , value (v)
    , location (l)
  {}

  template <typename Base>
  TParser::basic_symbol<Base>::basic_symbol (typename Base::kind_type t, const ui64 v, const location_type& l)
    : Base (t)
    , value (v)
    , location (l)
  {}


  template <typename Base>
  inline
  TParser::basic_symbol<Base>::~basic_symbol ()
  {
    // User destructor.
    symbol_number_type yytype = this->type_get ();
    switch (yytype)
    {
   default:
      break;
    }

    // Type destructor.
    switch (yytype)
    {
      case 59: // relational-op
      case 61: // additive-op
      case 63: // multiplicative-op
        value.template destroy< EBinaryOp > ();
        break;

      case 66: // unary-op
        value.template destroy< EUnaryOp > ();
        break;

      case 23: // "string literal"
        value.template destroy< Stroka > ();
        break;

      case 49: // where-clause
      case 55: // expression
      case 56: // or-op-expr
      case 57: // and-op-expr
      case 58: // relational-op-expr
      case 60: // additive-op-expr
      case 62: // multiplicative-op-expr
      case 64: // comma-expr
      case 65: // unary-expr
      case 67: // atomic-expr
        value.template destroy< TExpressionPtr > ();
        break;

      case 48: // identifier-list
      case 51: // order-by-clause
        value.template destroy< TIdentifierList > ();
        break;

      case 54: // named-expression
        value.template destroy< TNamedExpression > ();
        break;

      case 50: // group-by-clause
      case 53: // named-expression-list
        value.template destroy< TNamedExpressionList > ();
        break;

      case 41: // select-clause
        value.template destroy< TNullableNamedExprs > ();
        break;

      case 19: // "identifier"
        value.template destroy< TStringBuf > ();
        break;

      case 68: // literal-expr
        value.template destroy< TUnversionedValue > ();
        break;

      case 69: // literal-list
      case 70: // literal-tuple
        value.template destroy< TValueList > ();
        break;

      case 71: // literal-tuple-list
        value.template destroy< TValueTupleList > ();
        break;

      case 22: // "double literal"
        value.template destroy< double > ();
        break;

      case 20: // "int64 literal"
      case 52: // limit-clause
        value.template destroy< i64 > ();
        break;

      case 21: // "uint64 literal"
        value.template destroy< ui64 > ();
        break;

      default:
        break;
    }

  }

  template <typename Base>
  inline
  void
  TParser::basic_symbol<Base>::move (basic_symbol& s)
  {
    super_type::move(s);
      switch (this->type_get ())
    {
      case 59: // relational-op
      case 61: // additive-op
      case 63: // multiplicative-op
        value.move< EBinaryOp > (s.value);
        break;

      case 66: // unary-op
        value.move< EUnaryOp > (s.value);
        break;

      case 23: // "string literal"
        value.move< Stroka > (s.value);
        break;

      case 49: // where-clause
      case 55: // expression
      case 56: // or-op-expr
      case 57: // and-op-expr
      case 58: // relational-op-expr
      case 60: // additive-op-expr
      case 62: // multiplicative-op-expr
      case 64: // comma-expr
      case 65: // unary-expr
      case 67: // atomic-expr
        value.move< TExpressionPtr > (s.value);
        break;

      case 48: // identifier-list
      case 51: // order-by-clause
        value.move< TIdentifierList > (s.value);
        break;

      case 54: // named-expression
        value.move< TNamedExpression > (s.value);
        break;

      case 50: // group-by-clause
      case 53: // named-expression-list
        value.move< TNamedExpressionList > (s.value);
        break;

      case 41: // select-clause
        value.move< TNullableNamedExprs > (s.value);
        break;

      case 19: // "identifier"
        value.move< TStringBuf > (s.value);
        break;

      case 68: // literal-expr
        value.move< TUnversionedValue > (s.value);
        break;

      case 69: // literal-list
      case 70: // literal-tuple
        value.move< TValueList > (s.value);
        break;

      case 71: // literal-tuple-list
        value.move< TValueTupleList > (s.value);
        break;

      case 22: // "double literal"
        value.move< double > (s.value);
        break;

      case 20: // "int64 literal"
      case 52: // limit-clause
        value.move< i64 > (s.value);
        break;

      case 21: // "uint64 literal"
        value.move< ui64 > (s.value);
        break;

      default:
        break;
    }

    location = s.location;
  }

  // by_type.
  inline
  TParser::by_type::by_type ()
     : type (empty)
  {}

  inline
  TParser::by_type::by_type (const by_type& other)
    : type (other.type)
  {}

  inline
  TParser::by_type::by_type (token_type t)
    : type (yytranslate_ (t))
  {}

  inline
  void
  TParser::by_type::move (by_type& that)
  {
    type = that.type;
    that.type = empty;
  }

  inline
  int
  TParser::by_type::type_get () const
  {
    return type;
  }
  // Implementation of make_symbol for each symbol type.
  TParser::symbol_type
  TParser::make_End (const location_type& l)
  {
    return symbol_type (token::End, l);

  }

  TParser::symbol_type
  TParser::make_Failure (const location_type& l)
  {
    return symbol_type (token::Failure, l);

  }

  TParser::symbol_type
  TParser::make_StrayWillParseQuery (const location_type& l)
  {
    return symbol_type (token::StrayWillParseQuery, l);

  }

  TParser::symbol_type
  TParser::make_StrayWillParseJobQuery (const location_type& l)
  {
    return symbol_type (token::StrayWillParseJobQuery, l);

  }

  TParser::symbol_type
  TParser::make_StrayWillParseExpression (const location_type& l)
  {
    return symbol_type (token::StrayWillParseExpression, l);

  }

  TParser::symbol_type
  TParser::make_KwFrom (const location_type& l)
  {
    return symbol_type (token::KwFrom, l);

  }

  TParser::symbol_type
  TParser::make_KwWhere (const location_type& l)
  {
    return symbol_type (token::KwWhere, l);

  }

  TParser::symbol_type
  TParser::make_KwLimit (const location_type& l)
  {
    return symbol_type (token::KwLimit, l);

  }

  TParser::symbol_type
  TParser::make_KwJoin (const location_type& l)
  {
    return symbol_type (token::KwJoin, l);

  }

  TParser::symbol_type
  TParser::make_KwUsing (const location_type& l)
  {
    return symbol_type (token::KwUsing, l);

  }

  TParser::symbol_type
  TParser::make_KwGroupBy (const location_type& l)
  {
    return symbol_type (token::KwGroupBy, l);

  }

  TParser::symbol_type
  TParser::make_KwOrderBy (const location_type& l)
  {
    return symbol_type (token::KwOrderBy, l);

  }

  TParser::symbol_type
  TParser::make_KwAs (const location_type& l)
  {
    return symbol_type (token::KwAs, l);

  }

  TParser::symbol_type
  TParser::make_KwAnd (const location_type& l)
  {
    return symbol_type (token::KwAnd, l);

  }

  TParser::symbol_type
  TParser::make_KwOr (const location_type& l)
  {
    return symbol_type (token::KwOr, l);

  }

  TParser::symbol_type
  TParser::make_KwBetween (const location_type& l)
  {
    return symbol_type (token::KwBetween, l);

  }

  TParser::symbol_type
  TParser::make_KwIn (const location_type& l)
  {
    return symbol_type (token::KwIn, l);

  }

  TParser::symbol_type
  TParser::make_Identifier (const TStringBuf& v, const location_type& l)
  {
    return symbol_type (token::Identifier, v, l);

  }

  TParser::symbol_type
  TParser::make_Int64Literal (const i64& v, const location_type& l)
  {
    return symbol_type (token::Int64Literal, v, l);

  }

  TParser::symbol_type
  TParser::make_Uint64Literal (const ui64& v, const location_type& l)
  {
    return symbol_type (token::Uint64Literal, v, l);

  }

  TParser::symbol_type
  TParser::make_DoubleLiteral (const double& v, const location_type& l)
  {
    return symbol_type (token::DoubleLiteral, v, l);

  }

  TParser::symbol_type
  TParser::make_StringLiteral (const Stroka& v, const location_type& l)
  {
    return symbol_type (token::StringLiteral, v, l);

  }

  TParser::symbol_type
  TParser::make_OpModulo (const location_type& l)
  {
    return symbol_type (token::OpModulo, l);

  }

  TParser::symbol_type
  TParser::make_LeftParenthesis (const location_type& l)
  {
    return symbol_type (token::LeftParenthesis, l);

  }

  TParser::symbol_type
  TParser::make_RightParenthesis (const location_type& l)
  {
    return symbol_type (token::RightParenthesis, l);

  }

  TParser::symbol_type
  TParser::make_Asterisk (const location_type& l)
  {
    return symbol_type (token::Asterisk, l);

  }

  TParser::symbol_type
  TParser::make_OpPlus (const location_type& l)
  {
    return symbol_type (token::OpPlus, l);

  }

  TParser::symbol_type
  TParser::make_Comma (const location_type& l)
  {
    return symbol_type (token::Comma, l);

  }

  TParser::symbol_type
  TParser::make_OpMinus (const location_type& l)
  {
    return symbol_type (token::OpMinus, l);

  }

  TParser::symbol_type
  TParser::make_OpDivide (const location_type& l)
  {
    return symbol_type (token::OpDivide, l);

  }

  TParser::symbol_type
  TParser::make_OpLess (const location_type& l)
  {
    return symbol_type (token::OpLess, l);

  }

  TParser::symbol_type
  TParser::make_OpLessOrEqual (const location_type& l)
  {
    return symbol_type (token::OpLessOrEqual, l);

  }

  TParser::symbol_type
  TParser::make_OpEqual (const location_type& l)
  {
    return symbol_type (token::OpEqual, l);

  }

  TParser::symbol_type
  TParser::make_OpNotEqual (const location_type& l)
  {
    return symbol_type (token::OpNotEqual, l);

  }

  TParser::symbol_type
  TParser::make_OpGreater (const location_type& l)
  {
    return symbol_type (token::OpGreater, l);

  }

  TParser::symbol_type
  TParser::make_OpGreaterOrEqual (const location_type& l)
  {
    return symbol_type (token::OpGreaterOrEqual, l);

  }



  // by_state.
  inline
  TParser::by_state::by_state ()
    : state (empty)
  {}

  inline
  TParser::by_state::by_state (const by_state& other)
    : state (other.state)
  {}

  inline
  void
  TParser::by_state::move (by_state& that)
  {
    state = that.state;
    that.state = empty;
  }

  inline
  TParser::by_state::by_state (state_type s)
    : state (s)
  {}

  inline
  TParser::symbol_number_type
  TParser::by_state::type_get () const
  {
    return state == empty ? 0 : yystos_[state];
  }

  inline
  TParser::stack_symbol_type::stack_symbol_type ()
  {}


  inline
  TParser::stack_symbol_type::stack_symbol_type (state_type s, symbol_type& that)
    : super_type (s, that.location)
  {
      switch (that.type_get ())
    {
      case 59: // relational-op
      case 61: // additive-op
      case 63: // multiplicative-op
        value.move< EBinaryOp > (that.value);
        break;

      case 66: // unary-op
        value.move< EUnaryOp > (that.value);
        break;

      case 23: // "string literal"
        value.move< Stroka > (that.value);
        break;

      case 49: // where-clause
      case 55: // expression
      case 56: // or-op-expr
      case 57: // and-op-expr
      case 58: // relational-op-expr
      case 60: // additive-op-expr
      case 62: // multiplicative-op-expr
      case 64: // comma-expr
      case 65: // unary-expr
      case 67: // atomic-expr
        value.move< TExpressionPtr > (that.value);
        break;

      case 48: // identifier-list
      case 51: // order-by-clause
        value.move< TIdentifierList > (that.value);
        break;

      case 54: // named-expression
        value.move< TNamedExpression > (that.value);
        break;

      case 50: // group-by-clause
      case 53: // named-expression-list
        value.move< TNamedExpressionList > (that.value);
        break;

      case 41: // select-clause
        value.move< TNullableNamedExprs > (that.value);
        break;

      case 19: // "identifier"
        value.move< TStringBuf > (that.value);
        break;

      case 68: // literal-expr
        value.move< TUnversionedValue > (that.value);
        break;

      case 69: // literal-list
      case 70: // literal-tuple
        value.move< TValueList > (that.value);
        break;

      case 71: // literal-tuple-list
        value.move< TValueTupleList > (that.value);
        break;

      case 22: // "double literal"
        value.move< double > (that.value);
        break;

      case 20: // "int64 literal"
      case 52: // limit-clause
        value.move< i64 > (that.value);
        break;

      case 21: // "uint64 literal"
        value.move< ui64 > (that.value);
        break;

      default:
        break;
    }

    // that is emptied.
    that.type = empty;
  }

  inline
  TParser::stack_symbol_type&
  TParser::stack_symbol_type::operator= (const stack_symbol_type& that)
  {
    state = that.state;
      switch (that.type_get ())
    {
      case 59: // relational-op
      case 61: // additive-op
      case 63: // multiplicative-op
        value.copy< EBinaryOp > (that.value);
        break;

      case 66: // unary-op
        value.copy< EUnaryOp > (that.value);
        break;

      case 23: // "string literal"
        value.copy< Stroka > (that.value);
        break;

      case 49: // where-clause
      case 55: // expression
      case 56: // or-op-expr
      case 57: // and-op-expr
      case 58: // relational-op-expr
      case 60: // additive-op-expr
      case 62: // multiplicative-op-expr
      case 64: // comma-expr
      case 65: // unary-expr
      case 67: // atomic-expr
        value.copy< TExpressionPtr > (that.value);
        break;

      case 48: // identifier-list
      case 51: // order-by-clause
        value.copy< TIdentifierList > (that.value);
        break;

      case 54: // named-expression
        value.copy< TNamedExpression > (that.value);
        break;

      case 50: // group-by-clause
      case 53: // named-expression-list
        value.copy< TNamedExpressionList > (that.value);
        break;

      case 41: // select-clause
        value.copy< TNullableNamedExprs > (that.value);
        break;

      case 19: // "identifier"
        value.copy< TStringBuf > (that.value);
        break;

      case 68: // literal-expr
        value.copy< TUnversionedValue > (that.value);
        break;

      case 69: // literal-list
      case 70: // literal-tuple
        value.copy< TValueList > (that.value);
        break;

      case 71: // literal-tuple-list
        value.copy< TValueTupleList > (that.value);
        break;

      case 22: // "double literal"
        value.copy< double > (that.value);
        break;

      case 20: // "int64 literal"
      case 52: // limit-clause
        value.copy< i64 > (that.value);
        break;

      case 21: // "uint64 literal"
        value.copy< ui64 > (that.value);
        break;

      default:
        break;
    }

    location = that.location;
    return *this;
  }


  template <typename Base>
  inline
  void
  TParser::yy_destroy_ (const char* yymsg, basic_symbol<Base>& yysym) const
  {
    if (yymsg)
      YY_SYMBOL_PRINT (yymsg, yysym);
  }

#if YT_QL_YYDEBUG
  template <typename Base>
  void
  TParser::yy_print_ (std::ostream& yyo,
                                     const basic_symbol<Base>& yysym) const
  {
    std::ostream& yyoutput = yyo;
    YYUSE (yyoutput);
    symbol_number_type yytype = yysym.type_get ();
    yyo << (yytype < yyntokens_ ? "token" : "nterm")
        << ' ' << yytname_[yytype] << " ("
        << yysym.location << ": ";
    YYUSE (yytype);
    yyo << ')';
  }
#endif

  inline
  void
  TParser::yypush_ (const char* m, state_type s, symbol_type& sym)
  {
    stack_symbol_type t (s, sym);
    yypush_ (m, t);
  }

  inline
  void
  TParser::yypush_ (const char* m, stack_symbol_type& s)
  {
    if (m)
      YY_SYMBOL_PRINT (m, s);
    yystack_.push (s);
  }

  inline
  void
  TParser::yypop_ (unsigned int n)
  {
    yystack_.pop (n);
  }

#if YT_QL_YYDEBUG
  std::ostream&
  TParser::debug_stream () const
  {
    return *yycdebug_;
  }

  void
  TParser::set_debug_stream (std::ostream& o)
  {
    yycdebug_ = &o;
  }


  TParser::debug_level_type
  TParser::debug_level () const
  {
    return yydebug_;
  }

  void
  TParser::set_debug_level (debug_level_type l)
  {
    yydebug_ = l;
  }
#endif // YT_QL_YYDEBUG

  inline TParser::state_type
  TParser::yy_lr_goto_state_ (state_type yystate, int yylhs)
  {
    int yyr = yypgoto_[yylhs - yyntokens_] + yystate;
    if (0 <= yyr && yyr <= yylast_ && yycheck_[yyr] == yystate)
      return yytable_[yyr];
    else
      return yydefgoto_[yylhs - yyntokens_];
  }

  inline bool
  TParser::yy_pact_value_is_default_ (int yyvalue)
  {
    return yyvalue == yypact_ninf_;
  }

  inline bool
  TParser::yy_table_value_is_error_ (int yyvalue)
  {
    return yyvalue == yytable_ninf_;
  }

  int
  TParser::parse ()
  {
    /// Whether yyla contains a lookahead.
    bool yyempty = true;

    // State.
    int yyn;
    int yylen = 0;

    // Error handling.
    int yynerrs_ = 0;
    int yyerrstatus_ = 0;

    /// The lookahead symbol.
    symbol_type yyla;

    /// The locations where the error started and ended.
    stack_symbol_type yyerror_range[3];

    /// $$ and @$.
    stack_symbol_type yylhs;

    /// The return value of parse ().
    int yyresult;

    // FIXME: This shoud be completely indented.  It is not yet to
    // avoid gratuitous conflicts when merging into the master branch.
    try
      {
    YYCDEBUG << "Starting parse" << std::endl;


    /* Initialize the stack.  The initial state will be set in
       yynewstate, since the latter expects the semantical and the
       location values to have been already stored, initialize these
       stacks with a primary value.  */
    yystack_.clear ();
    yypush_ (YY_NULL, 0, yyla);

    // A new symbol was pushed on the stack.
  yynewstate:
    YYCDEBUG << "Entering state " << yystack_[0].state << std::endl;

    // Accept?
    if (yystack_[0].state == yyfinal_)
      goto yyacceptlab;

    goto yybackup;

    // Backup.
  yybackup:

    // Try to take a decision without lookahead.
    yyn = yypact_[yystack_[0].state];
    if (yy_pact_value_is_default_ (yyn))
      goto yydefault;

    // Read a lookahead token.
    if (yyempty)
      {
        YYCDEBUG << "Reading a token: ";
        try
          {
            yyla.type = yytranslate_ (yylex (&yyla.value, &yyla.location));
          }
        catch (const syntax_error& yyexc)
          {
            error (yyexc);
            goto yyerrlab1;
          }
        yyempty = false;
      }
    YY_SYMBOL_PRINT ("Next token is", yyla);

    /* If the proper action on seeing token YYLA.TYPE is to reduce or
       to detect an error, take that action.  */
    yyn += yyla.type_get ();
    if (yyn < 0 || yylast_ < yyn || yycheck_[yyn] != yyla.type_get ())
      goto yydefault;

    // Reduce or error.
    yyn = yytable_[yyn];
    if (yyn <= 0)
      {
        if (yy_table_value_is_error_ (yyn))
          goto yyerrlab;
        yyn = -yyn;
        goto yyreduce;
      }

    // Discard the token being shifted.
    yyempty = true;

    // Count tokens shifted since error; after three, turn off error status.
    if (yyerrstatus_)
      --yyerrstatus_;

    // Shift the lookahead token.
    yypush_ ("Shifting", yyn, yyla);
    goto yynewstate;

  /*-----------------------------------------------------------.
  | yydefault -- do the default action for the current state.  |
  `-----------------------------------------------------------*/
  yydefault:
    yyn = yydefact_[yystack_[0].state];
    if (yyn == 0)
      goto yyerrlab;
    goto yyreduce;

  /*-----------------------------.
  | yyreduce -- Do a reduction.  |
  `-----------------------------*/
  yyreduce:
    yylen = yyr2_[yyn];
    yylhs.state = yy_lr_goto_state_(yystack_[yylen].state, yyr1_[yyn]);
    /* Variants are always initialized to an empty instance of the
       correct type. The default $$=$1 action is NOT applied when using
       variants.  */
      switch (yyr1_[yyn])
    {
      case 59: // relational-op
      case 61: // additive-op
      case 63: // multiplicative-op
        yylhs.value.build< EBinaryOp > ();
        break;

      case 66: // unary-op
        yylhs.value.build< EUnaryOp > ();
        break;

      case 23: // "string literal"
        yylhs.value.build< Stroka > ();
        break;

      case 49: // where-clause
      case 55: // expression
      case 56: // or-op-expr
      case 57: // and-op-expr
      case 58: // relational-op-expr
      case 60: // additive-op-expr
      case 62: // multiplicative-op-expr
      case 64: // comma-expr
      case 65: // unary-expr
      case 67: // atomic-expr
        yylhs.value.build< TExpressionPtr > ();
        break;

      case 48: // identifier-list
      case 51: // order-by-clause
        yylhs.value.build< TIdentifierList > ();
        break;

      case 54: // named-expression
        yylhs.value.build< TNamedExpression > ();
        break;

      case 50: // group-by-clause
      case 53: // named-expression-list
        yylhs.value.build< TNamedExpressionList > ();
        break;

      case 41: // select-clause
        yylhs.value.build< TNullableNamedExprs > ();
        break;

      case 19: // "identifier"
        yylhs.value.build< TStringBuf > ();
        break;

      case 68: // literal-expr
        yylhs.value.build< TUnversionedValue > ();
        break;

      case 69: // literal-list
      case 70: // literal-tuple
        yylhs.value.build< TValueList > ();
        break;

      case 71: // literal-tuple-list
        yylhs.value.build< TValueTupleList > ();
        break;

      case 22: // "double literal"
        yylhs.value.build< double > ();
        break;

      case 20: // "int64 literal"
      case 52: // limit-clause
        yylhs.value.build< i64 > ();
        break;

      case 21: // "uint64 literal"
        yylhs.value.build< ui64 > ();
        break;

      default:
        break;
    }


    // Compute the default @$.
    {
      slice<stack_symbol_type, stack_type> slice (yystack_, yylen);
      YYLLOC_DEFAULT (yylhs.location, slice, yylen);
    }

    // Perform the reduction.
    YY_REDUCE_PRINT (yyn);
    try
      {
        switch (yyn)
          {
  case 4:
    {
            head->As<TNamedExpression>() = yystack_[0].value.as< TNamedExpression > ();
        }
    break;

  case 5:
    {
            head->As<TQuery>().SelectExprs = yystack_[1].value.as< TNullableNamedExprs > ();
        }
    break;

  case 6:
    {
            yylhs.value.as< TNullableNamedExprs > () = yystack_[0].value.as< TNamedExpressionList > ();
        }
    break;

  case 7:
    {
            yylhs.value.as< TNullableNamedExprs > () = TNullableNamedExprs();
        }
    break;

  case 8:
    {
            head->As<TQuery>().Source = New<TSimpleSource>(Stroka(yystack_[0].value.as< TStringBuf > ()));
        }
    break;

  case 9:
    {
            head->As<TQuery>().Source = New<TJoinSource>(Stroka(yystack_[4].value.as< TStringBuf > ()), Stroka(yystack_[2].value.as< TStringBuf > ()), yystack_[0].value.as< TIdentifierList > ());
        }
    break;

  case 12:
    {
            head->As<TQuery>().WherePredicate = yystack_[0].value.as< TExpressionPtr > ();
        }
    break;

  case 13:
    {
            head->As<TQuery>().WherePredicate = yystack_[1].value.as< TExpressionPtr > ();
        }
    break;

  case 15:
    {
            head->As<TQuery>().GroupExprs = yystack_[0].value.as< TNamedExpressionList > ();
        }
    break;

  case 16:
    {
            head->As<TQuery>().GroupExprs = yystack_[1].value.as< TNamedExpressionList > ();
        }
    break;

  case 18:
    {
            head->As<TQuery>().OrderFields = yystack_[0].value.as< TIdentifierList > ();
        }
    break;

  case 19:
    {
            head->As<TQuery>().OrderFields = yystack_[1].value.as< TIdentifierList > ();
        }
    break;

  case 21:
    {
            head->As<TQuery>().Limit = yystack_[0].value.as< i64 > ();
        }
    break;

  case 22:
    {
            yylhs.value.as< TIdentifierList > ().swap(yystack_[2].value.as< TIdentifierList > ());
            yylhs.value.as< TIdentifierList > ().push_back(Stroka(yystack_[0].value.as< TStringBuf > ()));
        }
    break;

  case 23:
    {
            yylhs.value.as< TIdentifierList > ().push_back(Stroka(yystack_[0].value.as< TStringBuf > ()));
        }
    break;

  case 24:
    {
            yylhs.value.as< TExpressionPtr > () = yystack_[0].value.as< TExpressionPtr > ();
        }
    break;

  case 25:
    {
            yylhs.value.as< TNamedExpressionList > () = yystack_[0].value.as< TNamedExpressionList > ();
        }
    break;

  case 26:
    {
            yylhs.value.as< TIdentifierList > () = yystack_[0].value.as< TIdentifierList > ();
        }
    break;

  case 27:
    {
            yylhs.value.as< i64 > () = yystack_[0].value.as< i64 > ();
        }
    break;

  case 28:
    {
            yylhs.value.as< TNamedExpressionList > ().swap(yystack_[2].value.as< TNamedExpressionList > ());
            yylhs.value.as< TNamedExpressionList > ().push_back(yystack_[0].value.as< TNamedExpression > ());
        }
    break;

  case 29:
    {
            yylhs.value.as< TNamedExpressionList > ().push_back(yystack_[0].value.as< TNamedExpression > ());
        }
    break;

  case 30:
    {
            yylhs.value.as< TNamedExpression > () = TNamedExpression(yystack_[0].value.as< TExpressionPtr > (), InferName(yystack_[0].value.as< TExpressionPtr > ().Get()));
        }
    break;

  case 31:
    {
            yylhs.value.as< TNamedExpression > () = TNamedExpression(yystack_[2].value.as< TExpressionPtr > (), Stroka(yystack_[0].value.as< TStringBuf > ()));
        }
    break;

  case 32:
    { yylhs.value.as< TExpressionPtr > () = yystack_[0].value.as< TExpressionPtr > (); }
    break;

  case 33:
    {
            yylhs.value.as< TExpressionPtr > () = New<TBinaryOpExpression>(yylhs.location, EBinaryOp::Or, yystack_[2].value.as< TExpressionPtr > (), yystack_[0].value.as< TExpressionPtr > ());
        }
    break;

  case 34:
    { yylhs.value.as< TExpressionPtr > () = yystack_[0].value.as< TExpressionPtr > (); }
    break;

  case 35:
    {
            yylhs.value.as< TExpressionPtr > () = New<TBinaryOpExpression>(yylhs.location, EBinaryOp::And, yystack_[2].value.as< TExpressionPtr > (), yystack_[0].value.as< TExpressionPtr > ());
        }
    break;

  case 36:
    { yylhs.value.as< TExpressionPtr > () = yystack_[0].value.as< TExpressionPtr > (); }
    break;

  case 37:
    {
            yylhs.value.as< TExpressionPtr > () = New<TBinaryOpExpression>(yylhs.location, yystack_[1].value.as< EBinaryOp > (), yystack_[2].value.as< TExpressionPtr > (), yystack_[0].value.as< TExpressionPtr > ());
        }
    break;

  case 38:
    {
            yylhs.value.as< TExpressionPtr > () = New<TBinaryOpExpression>(yylhs.location, EBinaryOp::And,
                New<TBinaryOpExpression>(yylhs.location, EBinaryOp::GreaterOrEqual, yystack_[4].value.as< TExpressionPtr > (), yystack_[2].value.as< TExpressionPtr > ()),
                New<TBinaryOpExpression>(yylhs.location, EBinaryOp::LessOrEqual, yystack_[4].value.as< TExpressionPtr > (), yystack_[0].value.as< TExpressionPtr > ()));

        }
    break;

  case 39:
    {
            yylhs.value.as< TExpressionPtr > () = New<TInExpression>(yylhs.location, yystack_[4].value.as< TExpressionPtr > (), yystack_[1].value.as< TValueTupleList > ());
        }
    break;

  case 40:
    { yylhs.value.as< TExpressionPtr > () = yystack_[0].value.as< TExpressionPtr > (); }
    break;

  case 41:
    { yylhs.value.as< EBinaryOp > () = EBinaryOp::Equal; }
    break;

  case 42:
    { yylhs.value.as< EBinaryOp > () = EBinaryOp::NotEqual; }
    break;

  case 43:
    { yylhs.value.as< EBinaryOp > () = EBinaryOp::Less; }
    break;

  case 44:
    { yylhs.value.as< EBinaryOp > () = EBinaryOp::LessOrEqual; }
    break;

  case 45:
    { yylhs.value.as< EBinaryOp > () = EBinaryOp::Greater; }
    break;

  case 46:
    { yylhs.value.as< EBinaryOp > () = EBinaryOp::GreaterOrEqual; }
    break;

  case 47:
    {
            yylhs.value.as< TExpressionPtr > () = New<TBinaryOpExpression>(yylhs.location, yystack_[1].value.as< EBinaryOp > (), yystack_[2].value.as< TExpressionPtr > (), yystack_[0].value.as< TExpressionPtr > ());
        }
    break;

  case 48:
    { yylhs.value.as< TExpressionPtr > () = yystack_[0].value.as< TExpressionPtr > (); }
    break;

  case 49:
    { yylhs.value.as< EBinaryOp > () = EBinaryOp::Plus; }
    break;

  case 50:
    { yylhs.value.as< EBinaryOp > () = EBinaryOp::Minus; }
    break;

  case 51:
    {
            yylhs.value.as< TExpressionPtr > () = New<TBinaryOpExpression>(yylhs.location, yystack_[1].value.as< EBinaryOp > (), yystack_[2].value.as< TExpressionPtr > (), yystack_[0].value.as< TExpressionPtr > ());
        }
    break;

  case 52:
    { yylhs.value.as< TExpressionPtr > () = yystack_[0].value.as< TExpressionPtr > (); }
    break;

  case 53:
    { yylhs.value.as< EBinaryOp > () = EBinaryOp::Multiply; }
    break;

  case 54:
    { yylhs.value.as< EBinaryOp > () = EBinaryOp::Divide; }
    break;

  case 55:
    { yylhs.value.as< EBinaryOp > () = EBinaryOp::Modulo; }
    break;

  case 56:
    {
            yylhs.value.as< TExpressionPtr > () = New<TCommaExpression>(yylhs.location, yystack_[2].value.as< TExpressionPtr > (), yystack_[0].value.as< TExpressionPtr > ());
        }
    break;

  case 57:
    { yylhs.value.as< TExpressionPtr > () = yystack_[0].value.as< TExpressionPtr > (); }
    break;

  case 58:
    {
            yylhs.value.as< TExpressionPtr > () = New<TUnaryOpExpression>(yylhs.location, yystack_[1].value.as< EUnaryOp > (), yystack_[0].value.as< TExpressionPtr > ());
        }
    break;

  case 59:
    { yylhs.value.as< TExpressionPtr > () = yystack_[0].value.as< TExpressionPtr > (); }
    break;

  case 60:
    { yylhs.value.as< EUnaryOp > () = EUnaryOp::Plus; }
    break;

  case 61:
    { yylhs.value.as< EUnaryOp > () = EUnaryOp::Minus; }
    break;

  case 62:
    {
            yylhs.value.as< TExpressionPtr > () = New<TReferenceExpression>(yylhs.location, yystack_[0].value.as< TStringBuf > ());
        }
    break;

  case 63:
    {
            yylhs.value.as< TExpressionPtr > () = New<TFunctionExpression>(yylhs.location, yystack_[3].value.as< TStringBuf > (), yystack_[1].value.as< TExpressionPtr > ());
        }
    break;

  case 64:
    {
            yylhs.value.as< TExpressionPtr > () = yystack_[1].value.as< TExpressionPtr > ();
        }
    break;

  case 65:
    {
            yylhs.value.as< TExpressionPtr > () = New<TLiteralExpression>(yylhs.location, yystack_[0].value.as< TUnversionedValue > ());
        }
    break;

  case 66:
    { yylhs.value.as< TUnversionedValue > () = MakeUnversionedInt64Value(yystack_[0].value.as< i64 > ()); }
    break;

  case 67:
    { yylhs.value.as< TUnversionedValue > () = MakeUnversionedUint64Value(yystack_[0].value.as< ui64 > ()); }
    break;

  case 68:
    { yylhs.value.as< TUnversionedValue > () = MakeUnversionedDoubleValue(yystack_[0].value.as< double > ()); }
    break;

  case 69:
    { yylhs.value.as< TUnversionedValue > () = rowBuffer->Capture(MakeUnversionedStringValue(yystack_[0].value.as< Stroka > ())); }
    break;

  case 70:
    {
            yylhs.value.as< TValueList > ().swap(yystack_[2].value.as< TValueList > ());
            yylhs.value.as< TValueList > ().push_back(yystack_[0].value.as< TUnversionedValue > ());
        }
    break;

  case 71:
    {
            yylhs.value.as< TValueList > ().push_back(yystack_[0].value.as< TUnversionedValue > ());
        }
    break;

  case 72:
    {
            yylhs.value.as< TValueList > ().push_back(yystack_[0].value.as< TUnversionedValue > ());
        }
    break;

  case 73:
    {
            yylhs.value.as< TValueList > () = yystack_[1].value.as< TValueList > ();
        }
    break;

  case 74:
    {
            yylhs.value.as< TValueTupleList > ().swap(yystack_[2].value.as< TValueTupleList > ());
            yylhs.value.as< TValueTupleList > ().push_back(yystack_[0].value.as< TValueList > ());
        }
    break;

  case 75:
    {
            yylhs.value.as< TValueTupleList > ().push_back(yystack_[0].value.as< TValueList > ());
        }
    break;


          default:
            break;
          }
      }
    catch (const syntax_error& yyexc)
      {
        error (yyexc);
        YYERROR;
      }
    YY_SYMBOL_PRINT ("-> $$ =", yylhs);
    yypop_ (yylen);
    yylen = 0;
    YY_STACK_PRINT ();

    // Shift the result of the reduction.
    yypush_ (YY_NULL, yylhs);
    goto yynewstate;

  /*--------------------------------------.
  | yyerrlab -- here on detecting error.  |
  `--------------------------------------*/
  yyerrlab:
    // If not already recovering from an error, report this error.
    if (!yyerrstatus_)
      {
        ++yynerrs_;
        error (yyla.location, yysyntax_error_ (yystack_[0].state,
                                           yyempty ? yyempty_ : yyla.type_get ()));
      }


    yyerror_range[1].location = yyla.location;
    if (yyerrstatus_ == 3)
      {
        /* If just tried and failed to reuse lookahead token after an
           error, discard it.  */

        // Return failure if at end of input.
        if (yyla.type_get () == yyeof_)
          YYABORT;
        else if (!yyempty)
          {
            yy_destroy_ ("Error: discarding", yyla);
            yyempty = true;
          }
      }

    // Else will try to reuse lookahead token after shifting the error token.
    goto yyerrlab1;


  /*---------------------------------------------------.
  | yyerrorlab -- error raised explicitly by YYERROR.  |
  `---------------------------------------------------*/
  yyerrorlab:

    /* Pacify compilers like GCC when the user code never invokes
       YYERROR and the label yyerrorlab therefore never appears in user
       code.  */
    if (false)
      goto yyerrorlab;
    yyerror_range[1].location = yystack_[yylen - 1].location;
    /* $$ was initialized before running the user action.  */
    YY_SYMBOL_PRINT ("Error: discarding", yylhs);
    yylhs.~stack_symbol_type();
    /* Do not reclaim the symbols of the rule whose action triggered
       this YYERROR.  */
    yypop_ (yylen);
    yylen = 0;
    goto yyerrlab1;

  /*-------------------------------------------------------------.
  | yyerrlab1 -- common code for both syntax error and YYERROR.  |
  `-------------------------------------------------------------*/
  yyerrlab1:
    yyerrstatus_ = 3;   // Each real token shifted decrements this.
    {
      stack_symbol_type error_token;
      for (;;)
        {
          yyn = yypact_[yystack_[0].state];
          if (!yy_pact_value_is_default_ (yyn))
            {
              yyn += yyterror_;
              if (0 <= yyn && yyn <= yylast_ && yycheck_[yyn] == yyterror_)
                {
                  yyn = yytable_[yyn];
                  if (0 < yyn)
                    break;
                }
            }

          // Pop the current state because it cannot handle the error token.
          if (yystack_.size () == 1)
            YYABORT;

          yyerror_range[1].location = yystack_[0].location;
          yy_destroy_ ("Error: popping", yystack_[0]);
          yypop_ ();
          YY_STACK_PRINT ();
        }

      yyerror_range[2].location = yyla.location;
      YYLLOC_DEFAULT (error_token.location, yyerror_range, 2);

      // Shift the error token.
      error_token.state = yyn;
      yypush_ ("Shifting", error_token);
    }
    goto yynewstate;

    // Accept.
  yyacceptlab:
    yyresult = 0;
    goto yyreturn;

    // Abort.
  yyabortlab:
    yyresult = 1;
    goto yyreturn;

  yyreturn:
    if (!yyempty)
      yy_destroy_ ("Cleanup: discarding lookahead", yyla);

    /* Do not reclaim the symbols of the rule whose action triggered
       this YYABORT or YYACCEPT.  */
    yypop_ (yylen);
    while (1 < yystack_.size ())
      {
        yy_destroy_ ("Cleanup: popping", yystack_[0]);
        yypop_ ();
      }

    return yyresult;
  }
    catch (...)
      {
        YYCDEBUG << "Exception caught: cleaning lookahead and stack"
                 << std::endl;
        // Do not try to display the values of the reclaimed symbols,
        // as their printer might throw an exception.
        if (!yyempty)
          yy_destroy_ (YY_NULL, yyla);

        while (1 < yystack_.size ())
          {
            yy_destroy_ (YY_NULL, yystack_[0]);
            yypop_ ();
          }
        throw;
      }
  }

  void
  TParser::error (const syntax_error& yyexc)
  {
    error (yyexc.location, yyexc.what());
  }

  // Generate an error message.
  std::string
  TParser::yysyntax_error_ (state_type yystate, symbol_number_type yytoken) const
  {
    std::string yyres;
    // Number of reported tokens (one for the "unexpected", one per
    // "expected").
    size_t yycount = 0;
    // Its maximum.
    enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
    // Arguments of yyformat.
    char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];

    /* There are many possibilities here to consider:
       - If this state is a consistent state with a default action, then
         the only way this function was invoked is if the default action
         is an error action.  In that case, don't check for expected
         tokens because there are none.
       - The only way there can be no lookahead present (in yytoken) is
         if this state is a consistent state with a default action.
         Thus, detecting the absence of a lookahead is sufficient to
         determine that there is no unexpected or expected token to
         report.  In that case, just report a simple "syntax error".
       - Don't assume there isn't a lookahead just because this state is
         a consistent state with a default action.  There might have
         been a previous inconsistent state, consistent state with a
         non-default action, or user semantic action that manipulated
         yyla.  (However, yyla is currently not documented for users.)
       - Of course, the expected token list depends on states to have
         correct lookahead information, and it depends on the parser not
         to perform extra reductions after fetching a lookahead from the
         scanner and before detecting a syntax error.  Thus, state
         merging (from LALR or IELR) and default reductions corrupt the
         expected token list.  However, the list is correct for
         canonical LR with one exception: it will still contain any
         token that will not be accepted due to an error action in a
         later state.
    */
    if (yytoken != yyempty_)
      {
        yyarg[yycount++] = yytname_[yytoken];
        int yyn = yypact_[yystate];
        if (!yy_pact_value_is_default_ (yyn))
          {
            /* Start YYX at -YYN if negative to avoid negative indexes in
               YYCHECK.  In other words, skip the first -YYN actions for
               this state because they are default actions.  */
            int yyxbegin = yyn < 0 ? -yyn : 0;
            // Stay within bounds of both yycheck and yytname.
            int yychecklim = yylast_ - yyn + 1;
            int yyxend = yychecklim < yyntokens_ ? yychecklim : yyntokens_;
            for (int yyx = yyxbegin; yyx < yyxend; ++yyx)
              if (yycheck_[yyx + yyn] == yyx && yyx != yyterror_
                  && !yy_table_value_is_error_ (yytable_[yyx + yyn]))
                {
                  if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
                    {
                      yycount = 1;
                      break;
                    }
                  else
                    yyarg[yycount++] = yytname_[yyx];
                }
          }
      }

    char const* yyformat = YY_NULL;
    switch (yycount)
      {
#define YYCASE_(N, S)                         \
        case N:                               \
          yyformat = S;                       \
        break
        YYCASE_(0, YY_("syntax error"));
        YYCASE_(1, YY_("syntax error, unexpected %s"));
        YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
        YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
        YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
        YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
#undef YYCASE_
      }

    // Argument number.
    size_t yyi = 0;
    for (char const* yyp = yyformat; *yyp; ++yyp)
      if (yyp[0] == '%' && yyp[1] == 's' && yyi < yycount)
        {
          yyres += yytnamerr_ (yyarg[yyi++]);
          ++yyp;
        }
      else
        yyres += *yyp;
    return yyres;
  }


  const signed char TParser::yypact_ninf_ = -81;

  const signed char TParser::yytable_ninf_ = -1;

  const signed char
  TParser::yypact_[] =
  {
      80,    -8,    -8,    12,    18,     0,   -81,   -81,   -81,   -81,
      12,   -81,   -81,   -81,   -81,    22,    10,   -81,    30,    40,
      33,    26,   -23,    14,    70,    32,   -81,   -81,   -81,   -81,
     -81,    12,   -81,    48,    57,    15,   -81,    12,    62,    12,
      12,   -81,   -81,   -81,   -81,   -81,   -81,    12,   -81,   -81,
      12,   -81,   -81,   -81,    12,    12,    43,   -81,    49,   -81,
      12,    79,    12,    71,    12,    73,   -81,   -81,   -81,   -81,
      34,    -3,    81,   -81,   -81,   -81,    33,    26,   -23,   -81,
      14,   -81,    78,    44,   -81,   -81,    75,    40,   -81,    10,
     -81,    66,   -81,   -81,   -81,    12,    50,   -81,   -81,    53,
      85,    82,   -81,   -81,    54,   -81,    44,    73,   -81,   -81,
      50,   -81,    66,   -81
  };

  const unsigned char
  TParser::yydefact_[] =
  {
       0,     0,     0,     0,     0,    62,    66,    67,    68,    69,
       0,     7,    60,    61,     2,     0,     6,    29,    30,    32,
      34,    36,    40,    48,    52,     0,    59,    65,     3,     4,
       1,     0,    57,     0,     0,    10,     5,     0,     0,     0,
       0,    43,    44,    41,    42,    45,    46,     0,    49,    50,
       0,    55,    53,    54,     0,     0,     0,    58,     0,    64,
       0,     8,     0,     0,     0,     0,    11,    14,    17,    20,
      12,    15,    18,    21,    28,    31,    33,    35,    37,    52,
      47,    51,     0,     0,    63,    56,     0,    24,    27,    25,
      23,    26,    13,    16,    19,     0,     0,    72,    75,     0,
       0,     0,    38,    71,     0,    39,     0,     0,    22,    73,
       0,    74,     9,    70
  };

  const signed char
  TParser::yypgoto_[] =
  {
     -81,   -81,    95,   -81,   -81,   -81,   -81,    28,    29,    27,
      -5,   -81,   -81,   -81,   -81,    39,    -1,   -10,    42,    67,
      65,   -81,    60,   -81,    58,   -81,    83,   -46,   -81,    84,
     -80,   -81,     4,   -81
  };

  const signed char
  TParser::yydefgoto_[] =
  {
      -1,     4,    14,    15,    35,    36,    66,    67,    68,    69,
      91,    70,    71,    72,    73,    16,    17,    18,    19,    20,
      21,    47,    22,    50,    23,    54,    33,    24,    25,    26,
      27,   104,    98,    99
  };

  const unsigned char
  TParser::yytable_[] =
  {
      32,    79,    29,    97,    79,    48,    63,    49,    81,    82,
      65,     5,     6,     7,     8,     9,   103,    10,    30,    11,
      12,    32,    13,    62,    63,    31,    97,    64,    65,    34,
     113,     5,     6,     7,     8,     9,    74,    10,    51,    37,
      12,    52,    13,    63,    38,    53,    64,    65,    40,   102,
      85,     5,     6,     7,     8,     9,    39,    10,    41,    42,
      43,    44,    45,    46,     6,     7,     8,     9,    83,    96,
       6,     7,     8,     9,    59,    84,    61,    60,    60,   105,
     109,    75,   106,   110,     1,     2,     3,    55,    56,    86,
      63,    88,    90,    95,   100,   101,   107,    28,    92,    94,
      93,   108,   112,    89,    87,    77,    76,    78,    80,    57,
     111,     0,     0,     0,    58
  };

  const signed char
  TParser::yycheck_[] =
  {
      10,    47,     3,    83,    50,    28,     9,    30,    54,    55,
      13,    19,    20,    21,    22,    23,    96,    25,     0,    27,
      28,    31,    30,     8,     9,    25,   106,    12,    13,     7,
     110,    19,    20,    21,    22,    23,    37,    25,    24,    29,
      28,    27,    30,     9,    14,    31,    12,    13,    15,    95,
      60,    19,    20,    21,    22,    23,    16,    25,    32,    33,
      34,    35,    36,    37,    20,    21,    22,    23,    25,    25,
      20,    21,    22,    23,    26,    26,    19,    29,    29,    26,
      26,    19,    29,    29,     4,     5,     6,    17,    18,    10,
       9,    20,    19,    15,    19,    29,    11,     2,    70,    72,
      71,    19,   107,    64,    62,    40,    39,    47,    50,    25,
     106,    -1,    -1,    -1,    31
  };

  const unsigned char
  TParser::yystos_[] =
  {
       0,     4,     5,     6,    39,    19,    20,    21,    22,    23,
      25,    27,    28,    30,    40,    41,    53,    54,    55,    56,
      57,    58,    60,    62,    65,    66,    67,    68,    40,    54,
       0,    25,    55,    64,     7,    42,    43,    29,    14,    16,
      15,    32,    33,    34,    35,    36,    37,    59,    28,    30,
      61,    24,    27,    31,    63,    17,    18,    67,    64,    26,
      29,    19,     8,     9,    12,    13,    44,    45,    46,    47,
      49,    50,    51,    52,    54,    19,    57,    58,    60,    65,
      62,    65,    65,    25,    26,    55,    10,    56,    20,    53,
      19,    48,    45,    46,    47,    15,    25,    68,    70,    71,
      19,    29,    65,    68,    69,    26,    29,    11,    19,    26,
      29,    70,    48,    68
  };

  const unsigned char
  TParser::yyr1_[] =
  {
       0,    38,    39,    39,    39,    40,    41,    41,    42,    42,
      43,    43,    44,    44,    44,    45,    45,    45,    46,    46,
      46,    47,    48,    48,    49,    50,    51,    52,    53,    53,
      54,    54,    55,    56,    56,    57,    57,    58,    58,    58,
      58,    59,    59,    59,    59,    59,    59,    60,    60,    61,
      61,    62,    62,    63,    63,    63,    64,    64,    65,    65,
      66,    66,    67,    67,    67,    67,    68,    68,    68,    68,
      69,    69,    70,    70,    71,    71
  };

  const unsigned char
  TParser::yyr2_[] =
  {
       0,     2,     2,     2,     2,     2,     1,     1,     2,     6,
       1,     2,     1,     2,     1,     1,     2,     1,     1,     2,
       1,     1,     3,     1,     2,     2,     2,     2,     3,     1,
       1,     3,     1,     3,     1,     3,     1,     3,     5,     5,
       1,     1,     1,     1,     1,     1,     1,     3,     1,     1,
       1,     3,     1,     1,     1,     1,     3,     1,     2,     1,
       1,     1,     1,     4,     3,     1,     1,     1,     1,     1,
       3,     1,     1,     3,     3,     1
  };



  // YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
  // First, the terminals, then, starting at \a yyntokens_, nonterminals.
  const char*
  const TParser::yytname_[] =
  {
  "\"end of stream\"", "error", "$undefined", "\"lexer failure\"",
  "StrayWillParseQuery", "StrayWillParseJobQuery",
  "StrayWillParseExpression", "\"keyword `FROM`\"", "\"keyword `WHERE`\"",
  "\"keyword `LIMIT`\"", "\"keyword `JOIN`\"", "\"keyword `USING`\"",
  "\"keyword `GROUP BY`\"", "\"keyword `ORDER BY`\"", "\"keyword `AS`\"",
  "\"keyword `AND`\"", "\"keyword `OR`\"", "\"keyword `BETWEEN`\"",
  "\"keyword `IN`\"", "\"identifier\"", "\"int64 literal\"",
  "\"uint64 literal\"", "\"double literal\"", "\"string literal\"",
  "\"`%`\"", "\"`(`\"", "\"`)`\"", "\"`*`\"", "\"`+`\"", "\"`,`\"",
  "\"`-`\"", "\"`/`\"", "\"`<`\"", "\"`<=`\"", "\"`=`\"", "\"`!=`\"",
  "\"`>`\"", "\"`>=`\"", "$accept", "head", "head-clause", "select-clause",
  "from-clause", "tail-clause-from", "tail-clause-where",
  "tail-clause-group-by", "tail-clause-order-by", "tail-clause-limit",
  "identifier-list", "where-clause", "group-by-clause", "order-by-clause",
  "limit-clause", "named-expression-list", "named-expression",
  "expression", "or-op-expr", "and-op-expr", "relational-op-expr",
  "relational-op", "additive-op-expr", "additive-op",
  "multiplicative-op-expr", "multiplicative-op", "comma-expr",
  "unary-expr", "unary-op", "atomic-expr", "literal-expr", "literal-list",
  "literal-tuple", "literal-tuple-list", YY_NULL
  };

#if YT_QL_YYDEBUG
  const unsigned short int
  TParser::yyrline_[] =
  {
       0,   140,   140,   141,   142,   149,   156,   160,   167,   171,
     178,   179,   182,   186,   190,   194,   198,   202,   206,   210,
     214,   218,   225,   230,   237,   244,   251,   258,   265,   270,
     277,   281,   288,   293,   297,   302,   306,   311,   315,   322,
     326,   331,   333,   335,   337,   339,   341,   346,   350,   355,
     357,   362,   366,   371,   373,   375,   380,   384,   389,   393,
     398,   400,   405,   409,   413,   417,   424,   426,   428,   430,
     435,   440,   447,   451,   458,   463
  };

  // Print the state stack on the debug stream.
  void
  TParser::yystack_print_ ()
  {
    *yycdebug_ << "Stack now";
    for (stack_type::const_iterator
           i = yystack_.begin (),
           i_end = yystack_.end ();
         i != i_end; ++i)
      *yycdebug_ << ' ' << i->state;
    *yycdebug_ << std::endl;
  }

  // Report on the debug stream that the rule \a yyrule is going to be reduced.
  void
  TParser::yy_reduce_print_ (int yyrule)
  {
    unsigned int yylno = yyrline_[yyrule];
    int yynrhs = yyr2_[yyrule];
    // Print the symbols being reduced, and their result.
    *yycdebug_ << "Reducing stack by rule " << yyrule - 1
               << " (line " << yylno << "):" << std::endl;
    // The symbols being reduced.
    for (int yyi = 0; yyi < yynrhs; yyi++)
      YY_SYMBOL_PRINT ("   $" << yyi + 1 << " =",
                       yystack_[(yynrhs) - (yyi + 1)]);
  }
#endif // YT_QL_YYDEBUG

  // Symbol number corresponding to token number t.
  inline
  TParser::token_number_type
  TParser::yytranslate_ (int t)
  {
    static
    const token_number_type
    translate_table[] =
    {
     0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,    24,     2,     2,
      25,    26,    27,    28,    29,    30,     2,    31,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
      32,    34,    36,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     3,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     6,     5,     4,
       1,     2,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    33,
      35,    37
    };
    const unsigned int user_token_number_max_ = 1021;
    const token_number_type undef_token_ = 2;

    if (static_cast<int>(t) <= yyeof_)
      return yyeof_;
    else if (static_cast<unsigned int> (t) <= user_token_number_max_)
      return translate_table[t];
    else
      return undef_token_;
  }

} } } // NYT::NQueryClient::NAst


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
