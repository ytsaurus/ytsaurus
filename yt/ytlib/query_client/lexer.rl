#include <yt/ytlib/query_client/lexer.h>

#include <util/system/defaults.h>
#include <util/string/cast.h>
#include <util/string/escape.h>

namespace NYT {
namespace NQueryClient {
namespace NAst {

////////////////////////////////////////////////////////////////////////////////

typedef TParser::token_type TToken;

////////////////////////////////////////////////////////////////////////////////

%%{
    machine Lexer;
    alphtype char;

    end = 0;
    all = ^0;
    wss = space+;

    kw_from = 'from'i;
    kw_where = 'where'i;
    kw_having = 'having'i;
    kw_limit = 'limit'i;
    kw_join = 'join'i;
    kw_using = 'using'i;
    kw_group_by = 'group'i wss 'by'i;
    kw_with_totals = 'with'i wss 'totals'i;
    kw_order_by = 'order'i wss 'by'i;
    kw_asc = 'asc'i;
    kw_desc = 'desc'i;
    kw_left = 'left'i;
    kw_as = 'as'i;
    kw_on = 'on'i;
    kw_and = 'and'i;
    kw_or = 'or'i;
    kw_not = 'not'i;
    kw_null = 'null'i;
    kw_between = 'between'i;
    kw_in = 'in'i;
    kw_transform = 'transform'i;
    kw_false = 'false'i;
    kw_true = 'true'i;
    kw_yson_false = '%false'i;
    kw_yson_true = '%true'i;

    keyword =
        kw_from | kw_where | kw_having |kw_limit | kw_join | kw_using | kw_group_by | kw_with_totals | kw_order_by
        | kw_asc | kw_desc | kw_left | kw_as | kw_on | kw_and | kw_or | kw_not | kw_null | kw_between | kw_in
        | kw_transform | kw_false | kw_true | kw_yson_false | kw_yson_true;

    identifier = [a-zA-Z_][a-zA-Z_0-9]* - keyword;

    fltexp = [Ee] [+\-]? digit+;
    fltdot = (digit* '.' digit+) | (digit+ '.' digit*);

    int64_literal = digit+;
    uint64_literal = digit+ 'u';
    double_literal = fltdot fltexp?;
    single_quoted_string = "'" ( [^'\\] | /\\./ )* "'";
    double_quoted_string = '"' ( [^"\\] | /\\./ )* '"';
    string_literal = single_quoted_string | double_quoted_string;

    quoted_identifier := |*
        '[' => {
            if (++rd == 1) {
                rs = fpc + 1;
            }
        };
        ']' => {
            if (--rd == 0) {
                re = fpc;
                type = TToken::Identifier;
                value->build(TStringBuf(rs, re));
                fnext main;
                fbreak;
            }
        };
        all;
    *|;

    main := |*

        kw_from => { type = TToken::KwFrom; fbreak; };
        kw_where => { type = TToken::KwWhere; fbreak; };
        kw_having => { type = TToken::KwHaving; fbreak; };
        kw_limit => { type = TToken::KwLimit; fbreak; };
        kw_join => { type = TToken::KwJoin; fbreak; };
        kw_using => { type = TToken::KwUsing; fbreak; };
        kw_group_by => { type = TToken::KwGroupBy; fbreak; };
        kw_with_totals => { type = TToken::KwWithTotals; fbreak; };
        kw_order_by => { type = TToken::KwOrderBy; fbreak; };
        kw_asc => { type = TToken::KwAsc; fbreak; };
        kw_desc => { type = TToken::KwDesc; fbreak; };
        kw_left => { type = TToken::KwLeft; fbreak; };
        kw_as => { type = TToken::KwAs; fbreak; };
        kw_on => { type = TToken::KwOn; fbreak; };
        kw_and => { type = TToken::KwAnd; fbreak; };
        kw_or => { type = TToken::KwOr; fbreak; };
        kw_not => { type = TToken::KwNot; fbreak; };
        kw_null => { type = TToken::KwNull; fbreak; };
        kw_between => { type = TToken::KwBetween; fbreak; };
        kw_in => { type = TToken::KwIn; fbreak; };
        kw_transform => { type = TToken::KwTransform; fbreak; };
        kw_false => { type = TToken::KwFalse; fbreak; };
        kw_true => { type = TToken::KwTrue; fbreak; };
        kw_yson_false => { type = TToken::KwFalse; fbreak; };
        kw_yson_true => { type = TToken::KwTrue; fbreak; };

        identifier => {
            type = TToken::Identifier;
            value->build(TStringBuf(ts, te));
            fbreak;
        };
        int64_literal => {
            type = TToken::Int64Literal;
            value->build(FromString<ui64>(ts, te - ts));
            fbreak;
        };
        uint64_literal => {
            type = TToken::Uint64Literal;
            value->build(FromString<ui64>(ts, te - ts - 1));
            fbreak;
        };
        double_literal => {
            type = TToken::DoubleLiteral;
            value->build(FromString<double>(ts, te - ts));
            fbreak;
        };
        string_literal => {
            type = TToken::StringLiteral;
            value->build(UnescapeC(ts + 1, te - ts - 2));
            fbreak;
        };

        '[' => {
            fhold;
            fgoto quoted_identifier;
        };
        ']' => {
            THROW_ERROR_EXCEPTION("Unexpected symbol \"]\" at position %v", ts - p);
        };

        '<=' => { type = TToken::OpLessOrEqual; fbreak; };
        '>=' => { type = TToken::OpGreaterOrEqual; fbreak; };
        '!=' => { type = TToken::OpNotEqual; fbreak; };
        '<<' => { type = TToken::OpLeftShift; fbreak; };
        '>>' => { type = TToken::OpRightShift; fbreak; };

        # Single-character tokens.
        [()*,<=>+-/%.|&~#] => {
            type = static_cast<TToken>(fc);
            fbreak;
        };

        end => { type = TToken::End; fbreak; };

        # Advance location pointers when skipping whitespace.
        wss => { location->first = te - s; };
    *|;

}%%

namespace {
%% write data;
} // namespace anonymous

TLexer::TLexer(
    const TString& source,
    TParser::token_type strayToken)
    : StrayToken_(strayToken)
    , InjectedStrayToken_(false)
    , p(nullptr)
    , pe(nullptr)
    , eof(nullptr)
    , rs(nullptr)
    , re(nullptr)
    , rd(0)
    , s(nullptr)
{
    Initialize(source.c_str(), source.c_str() + source.length());
}

void TLexer::Initialize(const char* begin, const char* end)
{
    p = s = begin;
    pe = eof = end;

    rs = re = nullptr;
    rd = 0;

    %% write init;
}

TParser::token_type TLexer::GetNextToken(
    TParser::semantic_type* value,
    TParser::location_type* location
)
{
    if (!InjectedStrayToken_) {
        InjectedStrayToken_ = true;
        location->first = 0;
        location->second = 0;
        return StrayToken_;
    }

    TParser::token_type type = TToken::End;

    location->first = p - s;
    %% write exec;
    location->second = p - s;

    if (cs == %%{ write error; }%%) {
        // TODO(sandello): Handle lexer failures.
        return TToken::Failure;
    } else {
        return type;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NAst
} // namespace NQueryClient
} // namespace NYT

