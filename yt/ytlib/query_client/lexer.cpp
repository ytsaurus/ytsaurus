
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




namespace {

static const int Lexer_start = 8;
static const int Lexer_first_final = 8;
static const int Lexer_error = 0;

static const int Lexer_en_quoted_identifier = 43;
static const int Lexer_en_main = 8;


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

    
	{
	cs = Lexer_start;
	ts = 0;
	te = 0;
	act = 0;
	}

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
    
	{
	if ( p == pe )
		goto _test_eof;
	goto _resume;

_again:
	switch ( cs ) {
		case 8: goto st8;
		case 0: goto st0;
		case 9: goto st9;
		case 1: goto st1;
		case 2: goto st2;
		case 3: goto st3;
		case 10: goto st10;
		case 11: goto st11;
		case 4: goto st4;
		case 5: goto st5;
		case 12: goto st12;
		case 13: goto st13;
		case 14: goto st14;
		case 15: goto st15;
		case 16: goto st16;
		case 17: goto st17;
		case 18: goto st18;
		case 19: goto st19;
		case 20: goto st20;
		case 21: goto st21;
		case 22: goto st22;
		case 23: goto st23;
		case 24: goto st24;
		case 25: goto st25;
		case 26: goto st26;
		case 27: goto st27;
		case 28: goto st28;
		case 29: goto st29;
		case 30: goto st30;
		case 31: goto st31;
		case 32: goto st32;
		case 6: goto st6;
		case 7: goto st7;
		case 33: goto st33;
		case 34: goto st34;
		case 35: goto st35;
		case 36: goto st36;
		case 37: goto st37;
		case 38: goto st38;
		case 39: goto st39;
		case 40: goto st40;
		case 41: goto st41;
		case 42: goto st42;
		case 43: goto st43;
	default: break;
	}

	if ( ++p == pe )
		goto _test_eof;
_resume:
	switch ( cs )
	{
tr0:
	{te = p+1;{ type = TToken::OpNotEqual; {p++; cs = 8; goto _out;} }}
	goto st8;
tr3:
	{te = p+1;{
            type = TToken::StringLiteral;
            value->build(UnescapeC(ts + 1, te - ts - 2));
            {p++; cs = 8; goto _out;}
        }}
	goto st8;
tr5:
	{{p = ((te))-1;}{
            type = TToken::DoubleLiteral;
            value->build(FromString<double>(ts, te - ts));
            {p++; cs = 8; goto _out;}
        }}
	goto st8;
tr8:
	{{p = ((te))-1;}{
            type = TToken::Identifier;
            value->build(TStringBuf(ts, te));
            {p++; cs = 8; goto _out;}
        }}
	goto st8;
tr11:
	{te = p+1;{ type = TToken::KwGroupBy; {p++; cs = 8; goto _out;} }}
	goto st8;
tr12:
	{te = p+1;{ type = TToken::End; {p++; cs = 8; goto _out;} }}
	goto st8;
tr15:
	{te = p+1;{
            type = static_cast<TToken>((*p));
            {p++; cs = 8; goto _out;}
        }}
	goto st8;
tr29:
	{te = p+1;{
            p--;
            {goto st43;}
        }}
	goto st8;
tr30:
	{te = p+1;{
            YUNREACHABLE();
        }}
	goto st8;
tr31:
	{te = p;p--;{ location->first = te - s; }}
	goto st8;
tr32:
	{te = p;p--;{
            type = static_cast<TToken>((*p));
            {p++; cs = 8; goto _out;}
        }}
	goto st8;
tr34:
	{te = p;p--;{
            type = TToken::DoubleLiteral;
            value->build(FromString<double>(ts, te - ts));
            {p++; cs = 8; goto _out;}
        }}
	goto st8;
tr36:
	{te = p;p--;{
            type = TToken::Int64Literal;
            value->build(FromString<i64>(ts, te - ts));
            {p++; cs = 8; goto _out;}
        }}
	goto st8;
tr37:
	{te = p+1;{
            type = TToken::Uint64Literal;
            value->build(FromString<ui64>(ts, te - ts - 1));
            {p++; cs = 8; goto _out;}
        }}
	goto st8;
tr38:
	{te = p+1;{ type = TToken::OpLessOrEqual; {p++; cs = 8; goto _out;} }}
	goto st8;
tr39:
	{te = p+1;{ type = TToken::OpGreaterOrEqual; {p++; cs = 8; goto _out;} }}
	goto st8;
tr40:
	{te = p;p--;{
            type = TToken::Identifier;
            value->build(TStringBuf(ts, te));
            {p++; cs = 8; goto _out;}
        }}
	goto st8;
tr43:
	{	switch( act ) {
	case 4:
	{{p = ((te))-1;} type = TToken::KwFrom; {p++; cs = 8; goto _out;} }
	break;
	case 5:
	{{p = ((te))-1;} type = TToken::KwWhere; {p++; cs = 8; goto _out;} }
	break;
	case 6:
	{{p = ((te))-1;} type = TToken::KwLimit; {p++; cs = 8; goto _out;} }
	break;
	case 8:
	{{p = ((te))-1;} type = TToken::KwAs; {p++; cs = 8; goto _out;} }
	break;
	case 9:
	{{p = ((te))-1;} type = TToken::KwAnd; {p++; cs = 8; goto _out;} }
	break;
	case 10:
	{{p = ((te))-1;} type = TToken::KwOr; {p++; cs = 8; goto _out;} }
	break;
	case 11:
	{{p = ((te))-1;} type = TToken::KwBetween; {p++; cs = 8; goto _out;} }
	break;
	case 12:
	{{p = ((te))-1;} type = TToken::KwIn; {p++; cs = 8; goto _out;} }
	break;
	case 13:
	{{p = ((te))-1;}
            type = TToken::Identifier;
            value->build(TStringBuf(ts, te));
            {p++; cs = 8; goto _out;}
        }
	break;
	}
	}
	goto st8;
st8:
	{ts = 0;}
	if ( ++p == pe )
		goto _test_eof8;
case 8:
	{ts = p;}
	switch( (*p) ) {
		case 0: goto tr12;
		case 32: goto st9;
		case 33: goto st1;
		case 34: goto st2;
		case 37: goto tr15;
		case 46: goto st10;
		case 60: goto st14;
		case 61: goto tr15;
		case 62: goto st15;
		case 65: goto st16;
		case 66: goto st19;
		case 70: goto st25;
		case 71: goto st28;
		case 73: goto st33;
		case 76: goto st34;
		case 79: goto st38;
		case 87: goto st39;
		case 91: goto tr29;
		case 93: goto tr30;
		case 95: goto tr22;
		case 97: goto st16;
		case 98: goto st19;
		case 102: goto st25;
		case 103: goto st28;
		case 105: goto st33;
		case 108: goto st34;
		case 111: goto st38;
		case 119: goto st39;
	}
	if ( (*p) < 48 ) {
		if ( (*p) > 13 ) {
			if ( 40 <= (*p) && (*p) <= 47 )
				goto tr15;
		} else if ( (*p) >= 9 )
			goto st9;
	} else if ( (*p) > 57 ) {
		if ( (*p) > 90 ) {
			if ( 99 <= (*p) && (*p) <= 122 )
				goto tr22;
		} else if ( (*p) >= 67 )
			goto tr22;
	} else
		goto st13;
	goto st0;
st0:
cs = 0;
	goto _out;
st9:
	if ( ++p == pe )
		goto _test_eof9;
case 9:
	if ( (*p) == 32 )
		goto st9;
	if ( 9 <= (*p) && (*p) <= 13 )
		goto st9;
	goto tr31;
st1:
	if ( ++p == pe )
		goto _test_eof1;
case 1:
	if ( (*p) == 61 )
		goto tr0;
	goto st0;
st2:
	if ( ++p == pe )
		goto _test_eof2;
case 2:
	switch( (*p) ) {
		case 34: goto tr3;
		case 92: goto st3;
	}
	goto st2;
st3:
	if ( ++p == pe )
		goto _test_eof3;
case 3:
	goto st2;
st10:
	if ( ++p == pe )
		goto _test_eof10;
case 10:
	if ( 48 <= (*p) && (*p) <= 57 )
		goto tr33;
	goto tr32;
tr33:
	{te = p+1;}
	goto st11;
st11:
	if ( ++p == pe )
		goto _test_eof11;
case 11:
	switch( (*p) ) {
		case 69: goto st4;
		case 101: goto st4;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto tr33;
	goto tr34;
st4:
	if ( ++p == pe )
		goto _test_eof4;
case 4:
	switch( (*p) ) {
		case 43: goto st5;
		case 45: goto st5;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st12;
	goto tr5;
st5:
	if ( ++p == pe )
		goto _test_eof5;
case 5:
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st12;
	goto tr5;
st12:
	if ( ++p == pe )
		goto _test_eof12;
case 12:
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st12;
	goto tr34;
st13:
	if ( ++p == pe )
		goto _test_eof13;
case 13:
	switch( (*p) ) {
		case 46: goto tr33;
		case 117: goto tr37;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st13;
	goto tr36;
st14:
	if ( ++p == pe )
		goto _test_eof14;
case 14:
	if ( (*p) == 61 )
		goto tr38;
	goto tr32;
st15:
	if ( ++p == pe )
		goto _test_eof15;
case 15:
	if ( (*p) == 61 )
		goto tr39;
	goto tr32;
st16:
	if ( ++p == pe )
		goto _test_eof16;
case 16:
	switch( (*p) ) {
		case 78: goto st18;
		case 83: goto tr42;
		case 95: goto tr22;
		case 110: goto st18;
		case 115: goto tr42;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
tr22:
	{te = p+1;}
	{act = 13;}
	goto st17;
tr42:
	{te = p+1;}
	{act = 8;}
	goto st17;
tr44:
	{te = p+1;}
	{act = 9;}
	goto st17;
tr50:
	{te = p+1;}
	{act = 11;}
	goto st17;
tr53:
	{te = p+1;}
	{act = 4;}
	goto st17;
tr58:
	{te = p+1;}
	{act = 12;}
	goto st17;
tr62:
	{te = p+1;}
	{act = 6;}
	goto st17;
tr63:
	{te = p+1;}
	{act = 10;}
	goto st17;
tr67:
	{te = p+1;}
	{act = 5;}
	goto st17;
st17:
	if ( ++p == pe )
		goto _test_eof17;
case 17:
	if ( (*p) == 95 )
		goto tr22;
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr43;
st18:
	if ( ++p == pe )
		goto _test_eof18;
case 18:
	switch( (*p) ) {
		case 68: goto tr44;
		case 95: goto tr22;
		case 100: goto tr44;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st19:
	if ( ++p == pe )
		goto _test_eof19;
case 19:
	switch( (*p) ) {
		case 69: goto st20;
		case 95: goto tr22;
		case 101: goto st20;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st20:
	if ( ++p == pe )
		goto _test_eof20;
case 20:
	switch( (*p) ) {
		case 84: goto st21;
		case 95: goto tr22;
		case 116: goto st21;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st21:
	if ( ++p == pe )
		goto _test_eof21;
case 21:
	switch( (*p) ) {
		case 87: goto st22;
		case 95: goto tr22;
		case 119: goto st22;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st22:
	if ( ++p == pe )
		goto _test_eof22;
case 22:
	switch( (*p) ) {
		case 69: goto st23;
		case 95: goto tr22;
		case 101: goto st23;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st23:
	if ( ++p == pe )
		goto _test_eof23;
case 23:
	switch( (*p) ) {
		case 69: goto st24;
		case 95: goto tr22;
		case 101: goto st24;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st24:
	if ( ++p == pe )
		goto _test_eof24;
case 24:
	switch( (*p) ) {
		case 78: goto tr50;
		case 95: goto tr22;
		case 110: goto tr50;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st25:
	if ( ++p == pe )
		goto _test_eof25;
case 25:
	switch( (*p) ) {
		case 82: goto st26;
		case 95: goto tr22;
		case 114: goto st26;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st26:
	if ( ++p == pe )
		goto _test_eof26;
case 26:
	switch( (*p) ) {
		case 79: goto st27;
		case 95: goto tr22;
		case 111: goto st27;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st27:
	if ( ++p == pe )
		goto _test_eof27;
case 27:
	switch( (*p) ) {
		case 77: goto tr53;
		case 95: goto tr22;
		case 109: goto tr53;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st28:
	if ( ++p == pe )
		goto _test_eof28;
case 28:
	switch( (*p) ) {
		case 82: goto st29;
		case 95: goto tr22;
		case 114: goto st29;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st29:
	if ( ++p == pe )
		goto _test_eof29;
case 29:
	switch( (*p) ) {
		case 79: goto st30;
		case 95: goto tr22;
		case 111: goto st30;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st30:
	if ( ++p == pe )
		goto _test_eof30;
case 30:
	switch( (*p) ) {
		case 85: goto st31;
		case 95: goto tr22;
		case 117: goto st31;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st31:
	if ( ++p == pe )
		goto _test_eof31;
case 31:
	switch( (*p) ) {
		case 80: goto tr57;
		case 95: goto tr22;
		case 112: goto tr57;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
tr57:
	{te = p+1;}
	goto st32;
st32:
	if ( ++p == pe )
		goto _test_eof32;
case 32:
	switch( (*p) ) {
		case 32: goto st6;
		case 95: goto tr22;
	}
	if ( (*p) < 48 ) {
		if ( 9 <= (*p) && (*p) <= 13 )
			goto st6;
	} else if ( (*p) > 57 ) {
		if ( (*p) > 90 ) {
			if ( 97 <= (*p) && (*p) <= 122 )
				goto tr22;
		} else if ( (*p) >= 65 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st6:
	if ( ++p == pe )
		goto _test_eof6;
case 6:
	switch( (*p) ) {
		case 32: goto st6;
		case 66: goto st7;
		case 98: goto st7;
	}
	if ( 9 <= (*p) && (*p) <= 13 )
		goto st6;
	goto tr8;
st7:
	if ( ++p == pe )
		goto _test_eof7;
case 7:
	switch( (*p) ) {
		case 89: goto tr11;
		case 121: goto tr11;
	}
	goto tr8;
st33:
	if ( ++p == pe )
		goto _test_eof33;
case 33:
	switch( (*p) ) {
		case 78: goto tr58;
		case 95: goto tr22;
		case 110: goto tr58;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st34:
	if ( ++p == pe )
		goto _test_eof34;
case 34:
	switch( (*p) ) {
		case 73: goto st35;
		case 95: goto tr22;
		case 105: goto st35;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st35:
	if ( ++p == pe )
		goto _test_eof35;
case 35:
	switch( (*p) ) {
		case 77: goto st36;
		case 95: goto tr22;
		case 109: goto st36;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st36:
	if ( ++p == pe )
		goto _test_eof36;
case 36:
	switch( (*p) ) {
		case 73: goto st37;
		case 95: goto tr22;
		case 105: goto st37;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st37:
	if ( ++p == pe )
		goto _test_eof37;
case 37:
	switch( (*p) ) {
		case 84: goto tr62;
		case 95: goto tr22;
		case 116: goto tr62;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st38:
	if ( ++p == pe )
		goto _test_eof38;
case 38:
	switch( (*p) ) {
		case 82: goto tr63;
		case 95: goto tr22;
		case 114: goto tr63;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st39:
	if ( ++p == pe )
		goto _test_eof39;
case 39:
	switch( (*p) ) {
		case 72: goto st40;
		case 95: goto tr22;
		case 104: goto st40;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st40:
	if ( ++p == pe )
		goto _test_eof40;
case 40:
	switch( (*p) ) {
		case 69: goto st41;
		case 95: goto tr22;
		case 101: goto st41;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st41:
	if ( ++p == pe )
		goto _test_eof41;
case 41:
	switch( (*p) ) {
		case 82: goto st42;
		case 95: goto tr22;
		case 114: goto st42;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
st42:
	if ( ++p == pe )
		goto _test_eof42;
case 42:
	switch( (*p) ) {
		case 69: goto tr67;
		case 95: goto tr22;
		case 101: goto tr67;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr22;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr22;
	} else
		goto tr22;
	goto tr40;
tr68:
	{te = p+1;}
	goto st43;
tr69:
	{te = p+1;{
            if (++rd == 1) {
                rs = p + 1;
            }
        }}
	goto st43;
tr70:
	cs = 43;
	{te = p+1;{
            if (--rd == 0) {
                re = p;
                type = TToken::Identifier;
                value->build(TStringBuf(rs, re));
                cs = 8;
                {p++; goto _out;}
            }
        }}
	goto _again;
st43:
	{ts = 0;}
	if ( ++p == pe )
		goto _test_eof43;
case 43:
	{ts = p;}
	switch( (*p) ) {
		case 0: goto st0;
		case 91: goto tr69;
		case 93: goto tr70;
	}
	goto tr68;
	}
	_test_eof8: cs = 8; goto _test_eof; 
	_test_eof9: cs = 9; goto _test_eof; 
	_test_eof1: cs = 1; goto _test_eof; 
	_test_eof2: cs = 2; goto _test_eof; 
	_test_eof3: cs = 3; goto _test_eof; 
	_test_eof10: cs = 10; goto _test_eof; 
	_test_eof11: cs = 11; goto _test_eof; 
	_test_eof4: cs = 4; goto _test_eof; 
	_test_eof5: cs = 5; goto _test_eof; 
	_test_eof12: cs = 12; goto _test_eof; 
	_test_eof13: cs = 13; goto _test_eof; 
	_test_eof14: cs = 14; goto _test_eof; 
	_test_eof15: cs = 15; goto _test_eof; 
	_test_eof16: cs = 16; goto _test_eof; 
	_test_eof17: cs = 17; goto _test_eof; 
	_test_eof18: cs = 18; goto _test_eof; 
	_test_eof19: cs = 19; goto _test_eof; 
	_test_eof20: cs = 20; goto _test_eof; 
	_test_eof21: cs = 21; goto _test_eof; 
	_test_eof22: cs = 22; goto _test_eof; 
	_test_eof23: cs = 23; goto _test_eof; 
	_test_eof24: cs = 24; goto _test_eof; 
	_test_eof25: cs = 25; goto _test_eof; 
	_test_eof26: cs = 26; goto _test_eof; 
	_test_eof27: cs = 27; goto _test_eof; 
	_test_eof28: cs = 28; goto _test_eof; 
	_test_eof29: cs = 29; goto _test_eof; 
	_test_eof30: cs = 30; goto _test_eof; 
	_test_eof31: cs = 31; goto _test_eof; 
	_test_eof32: cs = 32; goto _test_eof; 
	_test_eof6: cs = 6; goto _test_eof; 
	_test_eof7: cs = 7; goto _test_eof; 
	_test_eof33: cs = 33; goto _test_eof; 
	_test_eof34: cs = 34; goto _test_eof; 
	_test_eof35: cs = 35; goto _test_eof; 
	_test_eof36: cs = 36; goto _test_eof; 
	_test_eof37: cs = 37; goto _test_eof; 
	_test_eof38: cs = 38; goto _test_eof; 
	_test_eof39: cs = 39; goto _test_eof; 
	_test_eof40: cs = 40; goto _test_eof; 
	_test_eof41: cs = 41; goto _test_eof; 
	_test_eof42: cs = 42; goto _test_eof; 
	_test_eof43: cs = 43; goto _test_eof; 

	_test_eof: {}
	if ( p == eof )
	{
	switch ( cs ) {
	case 9: goto tr31;
	case 10: goto tr32;
	case 11: goto tr34;
	case 4: goto tr5;
	case 5: goto tr5;
	case 12: goto tr34;
	case 13: goto tr36;
	case 14: goto tr32;
	case 15: goto tr32;
	case 16: goto tr40;
	case 17: goto tr43;
	case 18: goto tr40;
	case 19: goto tr40;
	case 20: goto tr40;
	case 21: goto tr40;
	case 22: goto tr40;
	case 23: goto tr40;
	case 24: goto tr40;
	case 25: goto tr40;
	case 26: goto tr40;
	case 27: goto tr40;
	case 28: goto tr40;
	case 29: goto tr40;
	case 30: goto tr40;
	case 31: goto tr40;
	case 32: goto tr40;
	case 6: goto tr8;
	case 7: goto tr8;
	case 33: goto tr40;
	case 34: goto tr40;
	case 35: goto tr40;
	case 36: goto tr40;
	case 37: goto tr40;
	case 38: goto tr40;
	case 39: goto tr40;
	case 40: goto tr40;
	case 41: goto tr40;
	case 42: goto tr40;
	}
	}

	_out: {}
	}

    location->second = p - s;

    if (cs == 0) {
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

