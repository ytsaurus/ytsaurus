#include <ytlib/query_client/lexer.h>

#include <util/system/defaults.h>
#include <util/string/cast.h>
#include <util/string/escape.h>

namespace NYT {
namespace NQueryClient {

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

	kw_and = 'and'i;
	kw_or = 'or'i;

    keyword = kw_from | kw_where;
    identifier = [a-zA-Z_][a-zA-Z_0-9]* - keyword;

    fltexp = [Ee] [+\-]? digit+;
    fltdot = (digit* '.' digit+) | (digit+ '.' digit*);

    integer_literal = digit+;
    double_literal = fltdot fltexp?;
    # TODO(sandello): Strings are not supported by the moment.
    # string_literal = '"' ( [^"\\] | /\\./ )* '"';

    # YPath handling relies on continuous chunking.
    ypath := |*
        '[' => {
            if (++rd == 1) {
                rs = fpc + 1;
            }
        };
        ']' => {
            if (--rd == 0) {
                re = fpc;
                type = TToken::YPathLiteral;
                value->build(Context_->Capture(rs, re));
                fnext main;
                fbreak;
            }
        };
        all;
    *|;

    main := |*

        kw_from   => { type = TToken::KwFrom;   fbreak; };
        kw_where  => { type = TToken::KwWhere;  fbreak; };

		kw_and   => { type = TToken::KwAnd;   fbreak; };
        kw_or  => { type = TToken::KwOr;  fbreak; };

        identifier => {
            type = TToken::Identifier;
            value->build(Context_->Capture(ts, te));
            fbreak;
        };
        integer_literal => {
            type = TToken::IntegerLiteral;
            value->build(FromString<i64>(ts, te - ts));
            fbreak;
        };
        double_literal => {
            type = TToken::DoubleLiteral;
            value->build(FromString<double>(ts, te - ts));
            fbreak;
        };
        # TODO(sandello): Strings are not supported by the moment.
        # string_literal => {
        #     type = TToken::StringLiteral;
        #     value->build<Stroka>(UnescapeC(ts, te - ts));
        #     fbreak;
        # };

        '[' => {
            fhold;
            fgoto ypath;
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
        wss => { location->begin = te - s; };
    *|;

}%%

namespace {
%% write data;
} // namespace anonymous

TLexer::TLexer(TPlanContext* context, const Stroka& source)
    : Context_(context)
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
    TParser::token_type type = TToken::End;

    location->begin = p - s;
    %% write exec;
    location->end = p - s;

    if (cs == %%{ write error; }%%) {
        // TODO(sandello): Handle lexer failures.
        return TToken::Failure;
    } else {
        return type;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

