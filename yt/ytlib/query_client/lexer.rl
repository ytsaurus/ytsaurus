#include <ytlib/query_client/lexer.h>

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
    kw_limit = 'limit'i;
    kw_join = 'join'i;
    kw_using = 'using'i;
    kw_group_by = 'group'i wss 'by'i;
    kw_as = 'as'i;
    kw_and = 'and'i;
    kw_or = 'or'i;
    kw_between = 'between'i;
    kw_in = 'in'i;

    keyword = kw_from | kw_where | kw_limit | kw_join | kw_using |kw_group_by | kw_as | kw_and | kw_or | kw_between | kw_in ;
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
        kw_limit => { type = TToken::KwLimit; fbreak; };
        kw_join => { type = TToken::KwJoin; fbreak; };
        kw_using => { type = TToken::KwUsing; fbreak; };
        kw_group_by => { type = TToken::KwGroupBy; fbreak; };
        kw_as => { type = TToken::KwAs; fbreak; };
        kw_and => { type = TToken::KwAnd; fbreak; };
        kw_or => { type = TToken::KwOr; fbreak; };
        kw_between => { type = TToken::KwBetween; fbreak; };
        kw_in => { type = TToken::KwIn; fbreak; };

        identifier => {
            type = TToken::Identifier;
            value->build(TStringBuf(ts, te));
            fbreak;
        };
        int64_literal => {
            type = TToken::Int64Literal;
            value->build(FromString<i64>(ts, te - ts));
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
            YUNREACHABLE();
        };

        '<=' => { type = TToken::OpLessOrEqual; fbreak; };
        '>=' => { type = TToken::OpGreaterOrEqual; fbreak; };
        '!=' => { type = TToken::OpNotEqual; fbreak; };

        # Single-character tokens.
        [()*,<=>+-/%] => {
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
    const Stroka& source,
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

