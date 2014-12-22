// A Bison parser, made by GNU Bison 3.0.2.

// Skeleton interface for Bison LALR(1) parsers in C++

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

/**
 ** \file /home/savrus/dev/yt/expression/yt/ytlib/query_client/parser.hpp
 ** Define the NYT::NQueryClient::NAst::parser class.
 */

// C++ LALR(1) parser skeleton written by Akim Demaille.

#ifndef YY_YT_QL_YY_HOME_SAVRUS_DEV_YT_EXPRESSION_YT_YTLIB_QUERY_CLIENT_PARSER_HPP_INCLUDED
# define YY_YT_QL_YY_HOME_SAVRUS_DEV_YT_EXPRESSION_YT_YTLIB_QUERY_CLIENT_PARSER_HPP_INCLUDED
// //                    "%code requires" blocks.

    #include "ast.h"

    namespace NYT { namespace NQueryClient { namespace NAst {
        using namespace NVersionedTableClient;

        class TLexer;
        class TParser;
    } } }



# include <vector>
# include <iostream>
# include <stdexcept>
# include <string>
# include "stack.hh"


#ifndef YYASSERT
# include <cassert>
# define YYASSERT assert
#endif


#ifndef YY_ATTRIBUTE
# if (defined __GNUC__                                               \
      && (2 < __GNUC__ || (__GNUC__ == 2 && 96 <= __GNUC_MINOR__)))  \
     || defined __SUNPRO_C && 0x5110 <= __SUNPRO_C
#  define YY_ATTRIBUTE(Spec) __attribute__(Spec)
# else
#  define YY_ATTRIBUTE(Spec) /* empty */
# endif
#endif

#ifndef YY_ATTRIBUTE_PURE
# define YY_ATTRIBUTE_PURE   YY_ATTRIBUTE ((__pure__))
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# define YY_ATTRIBUTE_UNUSED YY_ATTRIBUTE ((__unused__))
#endif

#if !defined _Noreturn \
     && (!defined __STDC_VERSION__ || __STDC_VERSION__ < 201112)
# if defined _MSC_VER && 1200 <= _MSC_VER
#  define _Noreturn __declspec (noreturn)
# else
#  define _Noreturn YY_ATTRIBUTE ((__noreturn__))
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(E) ((void) (E))
#else
# define YYUSE(E) /* empty */
#endif

#if defined __GNUC__ && 407 <= __GNUC__ * 100 + __GNUC_MINOR__
/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN \
    _Pragma ("GCC diagnostic push") \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")\
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# define YY_IGNORE_MAYBE_UNINITIALIZED_END \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

/* Debug traces.  */
#ifndef YT_QL_YYDEBUG
# if defined YYDEBUG
#if YYDEBUG
#   define YT_QL_YYDEBUG 1
#  else
#   define YT_QL_YYDEBUG 0
#  endif
# else /* ! defined YYDEBUG */
#  define YT_QL_YYDEBUG 0
# endif /* ! defined YYDEBUG */
#endif  /* ! defined YT_QL_YYDEBUG */

namespace NYT { namespace NQueryClient { namespace NAst {



  /// A char[S] buffer to store and retrieve objects.
  ///
  /// Sort of a variant, but does not keep track of the nature
  /// of the stored data, since that knowledge is available
  /// via the current state.
  template <size_t S>
  struct variant
  {
    /// Type of *this.
    typedef variant<S> self_type;

    /// Empty construction.
    variant ()
    {}

    /// Construct and fill.
    template <typename T>
    variant (const T& t)
    {
      YYASSERT (sizeof (T) <= S);
      new (yyas_<T> ()) T (t);
    }

    /// Destruction, allowed only if empty.
    ~variant ()
    {}

    /// Instantiate an empty \a T in here.
    template <typename T>
    T&
    build ()
    {
      return *new (yyas_<T> ()) T;
    }

    /// Instantiate a \a T in here from \a t.
    template <typename T>
    T&
    build (const T& t)
    {
      return *new (yyas_<T> ()) T (t);
    }

    /// Accessor to a built \a T.
    template <typename T>
    T&
    as ()
    {
      return *yyas_<T> ();
    }

    /// Const accessor to a built \a T (for %printer).
    template <typename T>
    const T&
    as () const
    {
      return *yyas_<T> ();
    }

    /// Swap the content with \a other, of same type.
    ///
    /// Both variants must be built beforehand, because swapping the actual
    /// data requires reading it (with as()), and this is not possible on
    /// unconstructed variants: it would require some dynamic testing, which
    /// should not be the variant's responsability.
    /// Swapping between built and (possibly) non-built is done with
    /// variant::move ().
    template <typename T>
    void
    swap (self_type& other)
    {
      std::swap (as<T> (), other.as<T> ());
    }

    /// Move the content of \a other to this.
    ///
    /// Destroys \a other.
    template <typename T>
    void
    move (self_type& other)
    {
      build<T> ();
      swap<T> (other);
      other.destroy<T> ();
    }

    /// Copy the content of \a other to this.
    template <typename T>
    void
    copy (const self_type& other)
    {
      build<T> (other.as<T> ());
    }

    /// Destroy the stored \a T.
    template <typename T>
    void
    destroy ()
    {
      as<T> ().~T ();
    }

  private:
    /// Prohibit blind copies.
    self_type& operator=(const self_type&);
    variant (const self_type&);

    /// Accessor to raw memory as \a T.
    template <typename T>
    T*
    yyas_ ()
    {
      void *yyp = yybuffer_.yyraw;
      return static_cast<T*> (yyp);
     }

    /// Const accessor to raw memory as \a T.
    template <typename T>
    const T*
    yyas_ () const
    {
      const void *yyp = yybuffer_.yyraw;
      return static_cast<const T*> (yyp);
     }

    union
    {
      /// Strongest alignment constraints.
      long double yyalign_me;
      /// A buffer large enough to store any of the semantic values.
      char yyraw[S];
    } yybuffer_;
  };


  /// A Bison parser.
  class TParser
  {
  public:
#ifndef YT_QL_YYSTYPE
    /// An auxiliary type to compute the largest semantic type.
    union union_type
    {
      // relational-op
      // additive-op
      // multiplicative-op
      char dummy1[sizeof(EBinaryOp)];

      // "string literal"
      char dummy2[sizeof(Stroka)];

      // where-clause
      // expression
      // or-op-expr
      // and-op-expr
      // relational-op-expr
      // additive-op-expr
      // multiplicative-op-expr
      // comma-expr
      // atomic-expr
      char dummy3[sizeof(TExpressionPtr)];

      // named-expression
      char dummy4[sizeof(TNamedExpression)];

      // group-by-clause
      // named-expression-list
      char dummy5[sizeof(TNamedExpressionList)];

      // select-clause
      char dummy6[sizeof(TNullableNamedExprs)];

      // "identifier"
      // from-clause
      char dummy7[sizeof(TStringBuf)];

      // literal-expr
      char dummy8[sizeof(TUnversionedValue)];

      // literal-list
      // literal-tuple
      char dummy9[sizeof(TValueList)];

      // literal-tuple-list
      char dummy10[sizeof(TValueTupleList)];

      // "double literal"
      char dummy11[sizeof(double)];

      // "int64 literal"
      // limit-clause
      char dummy12[sizeof(i64)];

      // "uint64 literal"
      char dummy13[sizeof(ui64)];
};

    /// Symbol semantic values.
    typedef variant<sizeof(union_type)> semantic_type;
#else
    typedef YT_QL_YYSTYPE semantic_type;
#endif
    /// Symbol locations.
    typedef TSourceLocation location_type;

    /// Syntax errors thrown from user actions.
    struct syntax_error : std::runtime_error
    {
      syntax_error (const location_type& l, const std::string& m);
      location_type location;
    };

    /// Tokens.
    struct token
    {
      enum yytokentype
      {
        End = 0,
        Failure = 256,
        StrayWillParseQuery = 999,
        StrayWillParseJobQuery = 998,
        StrayWillParseExpression = 997,
        KwFrom = 1002,
        KwWhere = 1003,
        KwLimit = 1004,
        KwGroupBy = 1005,
        KwAs = 1006,
        KwAnd = 1007,
        KwOr = 1008,
        KwBetween = 1009,
        KwIn = 1010,
        Identifier = 1011,
        Int64Literal = 1012,
        Uint64Literal = 1013,
        DoubleLiteral = 1014,
        StringLiteral = 1015,
        OpModulo = 37,
        LeftParenthesis = 40,
        RightParenthesis = 41,
        Asterisk = 42,
        OpPlus = 43,
        Comma = 44,
        OpMinus = 45,
        OpDivide = 47,
        OpLess = 60,
        OpLessOrEqual = 1016,
        OpEqual = 61,
        OpNotEqual = 1017,
        OpGreater = 62,
        OpGreaterOrEqual = 1018
      };
    };

    /// (External) token type, as returned by yylex.
    typedef token::yytokentype token_type;

    /// Internal symbol number.
    typedef int symbol_number_type;

    /// Internal symbol number for tokens (subsumed by symbol_number_type).
    typedef unsigned char token_number_type;

    /// A complete symbol.
    ///
    /// Expects its Base type to provide access to the symbol type
    /// via type_get().
    ///
    /// Provide access to semantic value and location.
    template <typename Base>
    struct basic_symbol : Base
    {
      /// Alias to Base.
      typedef Base super_type;

      /// Default constructor.
      basic_symbol ();

      /// Copy constructor.
      basic_symbol (const basic_symbol& other);

      /// Constructor for valueless symbols, and symbols from each type.

  basic_symbol (typename Base::kind_type t, const location_type& l);

  basic_symbol (typename Base::kind_type t, const EBinaryOp v, const location_type& l);

  basic_symbol (typename Base::kind_type t, const Stroka v, const location_type& l);

  basic_symbol (typename Base::kind_type t, const TExpressionPtr v, const location_type& l);

  basic_symbol (typename Base::kind_type t, const TNamedExpression v, const location_type& l);

  basic_symbol (typename Base::kind_type t, const TNamedExpressionList v, const location_type& l);

  basic_symbol (typename Base::kind_type t, const TNullableNamedExprs v, const location_type& l);

  basic_symbol (typename Base::kind_type t, const TStringBuf v, const location_type& l);

  basic_symbol (typename Base::kind_type t, const TUnversionedValue v, const location_type& l);

  basic_symbol (typename Base::kind_type t, const TValueList v, const location_type& l);

  basic_symbol (typename Base::kind_type t, const TValueTupleList v, const location_type& l);

  basic_symbol (typename Base::kind_type t, const double v, const location_type& l);

  basic_symbol (typename Base::kind_type t, const i64 v, const location_type& l);

  basic_symbol (typename Base::kind_type t, const ui64 v, const location_type& l);


      /// Constructor for symbols with semantic value.
      basic_symbol (typename Base::kind_type t,
                    const semantic_type& v,
                    const location_type& l);

      ~basic_symbol ();

      /// Destructive move, \a s is emptied into this.
      void move (basic_symbol& s);

      /// The semantic value.
      semantic_type value;

      /// The location.
      location_type location;

    private:
      /// Assignment operator.
      basic_symbol& operator= (const basic_symbol& other);
    };

    /// Type access provider for token (enum) based symbols.
    struct by_type
    {
      /// Default constructor.
      by_type ();

      /// Copy constructor.
      by_type (const by_type& other);

      /// The symbol type as needed by the constructor.
      typedef token_type kind_type;

      /// Constructor from (external) token numbers.
      by_type (kind_type t);

      /// Steal the symbol type from \a that.
      void move (by_type& that);

      /// The (internal) type number (corresponding to \a type).
      /// -1 when this symbol is empty.
      symbol_number_type type_get () const;

      /// The token.
      token_type token () const;

      enum { empty = 0 };

      /// The symbol type.
      /// -1 when this symbol is empty.
      token_number_type type;
    };

    /// "External" symbols: returned by the scanner.
    typedef basic_symbol<by_type> symbol_type;

    // Symbol constructors declarations.
    static inline
    symbol_type
    make_End (const location_type& l);

    static inline
    symbol_type
    make_Failure (const location_type& l);

    static inline
    symbol_type
    make_StrayWillParseQuery (const location_type& l);

    static inline
    symbol_type
    make_StrayWillParseJobQuery (const location_type& l);

    static inline
    symbol_type
    make_StrayWillParseExpression (const location_type& l);

    static inline
    symbol_type
    make_KwFrom (const location_type& l);

    static inline
    symbol_type
    make_KwWhere (const location_type& l);

    static inline
    symbol_type
    make_KwLimit (const location_type& l);

    static inline
    symbol_type
    make_KwGroupBy (const location_type& l);

    static inline
    symbol_type
    make_KwAs (const location_type& l);

    static inline
    symbol_type
    make_KwAnd (const location_type& l);

    static inline
    symbol_type
    make_KwOr (const location_type& l);

    static inline
    symbol_type
    make_KwBetween (const location_type& l);

    static inline
    symbol_type
    make_KwIn (const location_type& l);

    static inline
    symbol_type
    make_Identifier (const TStringBuf& v, const location_type& l);

    static inline
    symbol_type
    make_Int64Literal (const i64& v, const location_type& l);

    static inline
    symbol_type
    make_Uint64Literal (const ui64& v, const location_type& l);

    static inline
    symbol_type
    make_DoubleLiteral (const double& v, const location_type& l);

    static inline
    symbol_type
    make_StringLiteral (const Stroka& v, const location_type& l);

    static inline
    symbol_type
    make_OpModulo (const location_type& l);

    static inline
    symbol_type
    make_LeftParenthesis (const location_type& l);

    static inline
    symbol_type
    make_RightParenthesis (const location_type& l);

    static inline
    symbol_type
    make_Asterisk (const location_type& l);

    static inline
    symbol_type
    make_OpPlus (const location_type& l);

    static inline
    symbol_type
    make_Comma (const location_type& l);

    static inline
    symbol_type
    make_OpMinus (const location_type& l);

    static inline
    symbol_type
    make_OpDivide (const location_type& l);

    static inline
    symbol_type
    make_OpLess (const location_type& l);

    static inline
    symbol_type
    make_OpLessOrEqual (const location_type& l);

    static inline
    symbol_type
    make_OpEqual (const location_type& l);

    static inline
    symbol_type
    make_OpNotEqual (const location_type& l);

    static inline
    symbol_type
    make_OpGreater (const location_type& l);

    static inline
    symbol_type
    make_OpGreaterOrEqual (const location_type& l);


    /// Build a parser object.
    TParser (TLexer& lexer_yyarg, TAstHead* head_yyarg, TRowBuffer* rowBuffer_yyarg);
    virtual ~TParser ();

    /// Parse.
    /// \returns  0 iff parsing succeeded.
    virtual int parse ();

#if YT_QL_YYDEBUG
    /// The current debugging stream.
    std::ostream& debug_stream () const YY_ATTRIBUTE_PURE;
    /// Set the current debugging stream.
    void set_debug_stream (std::ostream &);

    /// Type for debugging levels.
    typedef int debug_level_type;
    /// The current debugging level.
    debug_level_type debug_level () const YY_ATTRIBUTE_PURE;
    /// Set the current debugging level.
    void set_debug_level (debug_level_type l);
#endif

    /// Report a syntax error.
    /// \param loc    where the syntax error is found.
    /// \param msg    a description of the syntax error.
    virtual void error (const location_type& loc, const std::string& msg);

    /// Report a syntax error.
    void error (const syntax_error& err);

  private:
    /// This class is not copyable.
    TParser (const TParser&);
    TParser& operator= (const TParser&);

    /// State numbers.
    typedef int state_type;

    /// Generate an error message.
    /// \param yystate   the state where the error occurred.
    /// \param yytoken   the lookahead token type, or yyempty_.
    virtual std::string yysyntax_error_ (state_type yystate,
                                         symbol_number_type yytoken) const;

    /// Compute post-reduction state.
    /// \param yystate   the current state
    /// \param yysym     the nonterminal to push on the stack
    state_type yy_lr_goto_state_ (state_type yystate, int yysym);

    /// Whether the given \c yypact_ value indicates a defaulted state.
    /// \param yyvalue   the value to check
    static bool yy_pact_value_is_default_ (int yyvalue);

    /// Whether the given \c yytable_ value indicates a syntax error.
    /// \param yyvalue   the value to check
    static bool yy_table_value_is_error_ (int yyvalue);

    static const signed char yypact_ninf_;
    static const signed char yytable_ninf_;

    /// Convert a scanner token number \a t to a symbol number.
    static token_number_type yytranslate_ (int t);

    // Tables.
  // YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
  // STATE-NUM.
  static const signed char yypact_[];

  // YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
  // Performed when YYTABLE does not specify something else to do.  Zero
  // means the default is an error.
  static const unsigned char yydefact_[];

  // YYPGOTO[NTERM-NUM].
  static const signed char yypgoto_[];

  // YYDEFGOTO[NTERM-NUM].
  static const signed char yydefgoto_[];

  // YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
  // positive, shift that token.  If negative, reduce the rule whose
  // number is the opposite.  If YYTABLE_NINF, syntax error.
  static const unsigned char yytable_[];

  static const signed char yycheck_[];

  // YYSTOS[STATE-NUM] -- The (internal number of the) accessing
  // symbol of state STATE-NUM.
  static const unsigned char yystos_[];

  // YYR1[YYN] -- Symbol number of symbol that rule YYN derives.
  static const unsigned char yyr1_[];

  // YYR2[YYN] -- Number of symbols on the right hand side of rule YYN.
  static const unsigned char yyr2_[];


    /// Convert the symbol name \a n to a form suitable for a diagnostic.
    static std::string yytnamerr_ (const char *n);


    /// For a symbol, its name in clear.
    static const char* const yytname_[];
#if YT_QL_YYDEBUG
  // YYRLINE[YYN] -- Source line where rule number YYN was defined.
  static const unsigned short int yyrline_[];
    /// Report on the debug stream that the rule \a r is going to be reduced.
    virtual void yy_reduce_print_ (int r);
    /// Print the state stack on the debug stream.
    virtual void yystack_print_ ();

    // Debugging.
    int yydebug_;
    std::ostream* yycdebug_;

    /// \brief Display a symbol type, value and location.
    /// \param yyo    The output stream.
    /// \param yysym  The symbol.
    template <typename Base>
    void yy_print_ (std::ostream& yyo, const basic_symbol<Base>& yysym) const;
#endif

    /// \brief Reclaim the memory associated to a symbol.
    /// \param yymsg     Why this token is reclaimed.
    ///                  If null, print nothing.
    /// \param yysym     The symbol.
    template <typename Base>
    void yy_destroy_ (const char* yymsg, basic_symbol<Base>& yysym) const;

  private:
    /// Type access provider for state based symbols.
    struct by_state
    {
      /// Default constructor.
      by_state ();

      /// The symbol type as needed by the constructor.
      typedef state_type kind_type;

      /// Constructor.
      by_state (kind_type s);

      /// Copy constructor.
      by_state (const by_state& other);

      /// Steal the symbol type from \a that.
      void move (by_state& that);

      /// The (internal) type number (corresponding to \a state).
      /// "empty" when empty.
      symbol_number_type type_get () const;

      enum { empty = 0 };

      /// The state.
      state_type state;
    };

    /// "Internal" symbol: element of the stack.
    struct stack_symbol_type : basic_symbol<by_state>
    {
      /// Superclass.
      typedef basic_symbol<by_state> super_type;
      /// Construct an empty symbol.
      stack_symbol_type ();
      /// Steal the contents from \a sym to build this.
      stack_symbol_type (state_type s, symbol_type& sym);
      /// Assignment, needed by push_back.
      stack_symbol_type& operator= (const stack_symbol_type& that);
    };

    /// Stack type.
    typedef stack<stack_symbol_type> stack_type;

    /// The stack.
    stack_type yystack_;

    /// Push a new state on the stack.
    /// \param m    a debug message to display
    ///             if null, no trace is output.
    /// \param s    the symbol
    /// \warning the contents of \a s.value is stolen.
    void yypush_ (const char* m, stack_symbol_type& s);

    /// Push a new look ahead token on the state on the stack.
    /// \param m    a debug message to display
    ///             if null, no trace is output.
    /// \param s    the state
    /// \param sym  the symbol (for its value and location).
    /// \warning the contents of \a s.value is stolen.
    void yypush_ (const char* m, state_type s, symbol_type& sym);

    /// Pop \a n symbols the three stacks.
    void yypop_ (unsigned int n = 1);

    // Constants.
    enum
    {
      yyeof_ = 0,
      yylast_ = 91,     ///< Last index in yytable_.
      yynnts_ = 27,  ///< Number of nonterminal symbols.
      yyempty_ = -2,
      yyfinal_ = 27, ///< Termination state number.
      yyterror_ = 1,
      yyerrcode_ = 256,
      yyntokens_ = 35  ///< Number of tokens.
    };


    // User arguments.
    TLexer& lexer;
    TAstHead* head;
    TRowBuffer* rowBuffer;
  };


} } } // NYT::NQueryClient::NAst




#endif // !YY_YT_QL_YY_HOME_SAVRUS_DEV_YT_EXPRESSION_YT_YTLIB_QUERY_CLIENT_PARSER_HPP_INCLUDED
