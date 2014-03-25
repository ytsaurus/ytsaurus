
#include <ytlib/query_client/lexer.h>

#include <util/system/defaults.h>
#include <util/string/cast.h>
#include <util/string/escape.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef TParser::token_type TToken;

////////////////////////////////////////////////////////////////////////////////




namespace {

static const int Lexer_start = 6;
static const int Lexer_first_final = 6;
static const int Lexer_error = 0;

static const int Lexer_en_ypath = 37;
static const int Lexer_en_main = 6;


} // namespace anonymous

TLexer::TLexer(
    TPlanContext* context,
    const Stroka& source,
    TParser::token_type strayToken)
    : Context_(context)
    , StrayToken_(strayToken)
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
		case 6: goto st6;
		case 0: goto st0;
		case 7: goto st7;
		case 1: goto st1;
		case 8: goto st8;
		case 9: goto st9;
		case 2: goto st2;
		case 3: goto st3;
		case 10: goto st10;
		case 11: goto st11;
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
		case 4: goto st4;
		case 5: goto st5;
		case 31: goto st31;
		case 32: goto st32;
		case 33: goto st33;
		case 34: goto st34;
		case 35: goto st35;
		case 36: goto st36;
		case 37: goto st37;
	default: break;
	}

	if ( ++p == pe )
		goto _test_eof;
_resume:
	switch ( cs )
	{
tr0:
	{te = p+1;{ type = TToken::OpNotEqual; {p++; cs = 6; goto _out;} }}
	goto st6;
tr2:
	{{p = ((te))-1;}{
            type = TToken::DoubleLiteral;
            value->build(FromString<double>(ts, te - ts));
            {p++; cs = 6; goto _out;}
        }}
	goto st6;
tr5:
	{{p = ((te))-1;}{
            type = TToken::Identifier;
            value->build(Context_->Capture(ts, te));
            {p++; cs = 6; goto _out;}
        }}
	goto st6;
tr8:
	{te = p+1;{ type = TToken::KwGroupBy; {p++; cs = 6; goto _out;} }}
	goto st6;
tr9:
	{te = p+1;{ type = TToken::End; {p++; cs = 6; goto _out;} }}
	goto st6;
tr12:
	{te = p+1;{
            type = static_cast<TToken>((*p));
            {p++; cs = 6; goto _out;}
        }}
	goto st6;
tr25:
	{te = p+1;{
            p--;
            {goto st37;}
        }}
	goto st6;
tr26:
	{te = p+1;{
            YUNREACHABLE();
        }}
	goto st6;
tr27:
	{te = p;p--;{ location->first = te - s; }}
	goto st6;
tr28:
	{te = p;p--;{
            type = static_cast<TToken>((*p));
            {p++; cs = 6; goto _out;}
        }}
	goto st6;
tr30:
	{te = p;p--;{
            type = TToken::DoubleLiteral;
            value->build(FromString<double>(ts, te - ts));
            {p++; cs = 6; goto _out;}
        }}
	goto st6;
tr32:
	{te = p;p--;{
            type = TToken::IntegerLiteral;
            value->build(FromString<i64>(ts, te - ts));
            {p++; cs = 6; goto _out;}
        }}
	goto st6;
tr33:
	{te = p+1;{ type = TToken::OpLessOrEqual; {p++; cs = 6; goto _out;} }}
	goto st6;
tr34:
	{te = p+1;{ type = TToken::OpGreaterOrEqual; {p++; cs = 6; goto _out;} }}
	goto st6;
tr35:
	{te = p;p--;{
            type = TToken::Identifier;
            value->build(Context_->Capture(ts, te));
            {p++; cs = 6; goto _out;}
        }}
	goto st6;
tr38:
	{	switch( act ) {
	case 4:
	{{p = ((te))-1;} type = TToken::KwFrom; {p++; cs = 6; goto _out;} }
	break;
	case 5:
	{{p = ((te))-1;} type = TToken::KwWhere; {p++; cs = 6; goto _out;} }
	break;
	case 7:
	{{p = ((te))-1;} type = TToken::KwAs; {p++; cs = 6; goto _out;} }
	break;
	case 8:
	{{p = ((te))-1;} type = TToken::KwAnd; {p++; cs = 6; goto _out;} }
	break;
	case 9:
	{{p = ((te))-1;} type = TToken::KwOr; {p++; cs = 6; goto _out;} }
	break;
	case 10:
	{{p = ((te))-1;} type = TToken::KwBetween; {p++; cs = 6; goto _out;} }
	break;
	case 11:
	{{p = ((te))-1;} type = TToken::KwIn; {p++; cs = 6; goto _out;} }
	break;
	case 12:
	{{p = ((te))-1;}
            type = TToken::Identifier;
            value->build(Context_->Capture(ts, te));
            {p++; cs = 6; goto _out;}
        }
	break;
	}
	}
	goto st6;
st6:
	{ts = 0;}
	if ( ++p == pe )
		goto _test_eof6;
case 6:
	{ts = p;}
	switch( (*p) ) {
		case 0: goto tr9;
		case 32: goto st7;
		case 33: goto st1;
		case 37: goto tr12;
		case 46: goto st8;
		case 60: goto st12;
		case 61: goto tr12;
		case 62: goto st13;
		case 65: goto st14;
		case 66: goto st17;
		case 70: goto st23;
		case 71: goto st26;
		case 73: goto st31;
		case 79: goto st32;
		case 87: goto st33;
		case 91: goto tr25;
		case 93: goto tr26;
		case 95: goto tr19;
		case 97: goto st14;
		case 98: goto st17;
		case 102: goto st23;
		case 103: goto st26;
		case 105: goto st31;
		case 111: goto st32;
		case 119: goto st33;
	}
	if ( (*p) < 48 ) {
		if ( (*p) > 13 ) {
			if ( 40 <= (*p) && (*p) <= 47 )
				goto tr12;
		} else if ( (*p) >= 9 )
			goto st7;
	} else if ( (*p) > 57 ) {
		if ( (*p) > 90 ) {
			if ( 99 <= (*p) && (*p) <= 122 )
				goto tr19;
		} else if ( (*p) >= 67 )
			goto tr19;
	} else
		goto st11;
	goto st0;
st0:
cs = 0;
	goto _out;
st7:
	if ( ++p == pe )
		goto _test_eof7;
case 7:
	if ( (*p) == 32 )
		goto st7;
	if ( 9 <= (*p) && (*p) <= 13 )
		goto st7;
	goto tr27;
st1:
	if ( ++p == pe )
		goto _test_eof1;
case 1:
	if ( (*p) == 61 )
		goto tr0;
	goto st0;
st8:
	if ( ++p == pe )
		goto _test_eof8;
case 8:
	if ( 48 <= (*p) && (*p) <= 57 )
		goto tr29;
	goto tr28;
tr29:
	{te = p+1;}
	goto st9;
st9:
	if ( ++p == pe )
		goto _test_eof9;
case 9:
	switch( (*p) ) {
		case 69: goto st2;
		case 101: goto st2;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto tr29;
	goto tr30;
st2:
	if ( ++p == pe )
		goto _test_eof2;
case 2:
	switch( (*p) ) {
		case 43: goto st3;
		case 45: goto st3;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st10;
	goto tr2;
st3:
	if ( ++p == pe )
		goto _test_eof3;
case 3:
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st10;
	goto tr2;
st10:
	if ( ++p == pe )
		goto _test_eof10;
case 10:
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st10;
	goto tr30;
st11:
	if ( ++p == pe )
		goto _test_eof11;
case 11:
	if ( (*p) == 46 )
		goto tr29;
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st11;
	goto tr32;
st12:
	if ( ++p == pe )
		goto _test_eof12;
case 12:
	if ( (*p) == 61 )
		goto tr33;
	goto tr28;
st13:
	if ( ++p == pe )
		goto _test_eof13;
case 13:
	if ( (*p) == 61 )
		goto tr34;
	goto tr28;
st14:
	if ( ++p == pe )
		goto _test_eof14;
case 14:
	switch( (*p) ) {
		case 78: goto st16;
		case 83: goto tr37;
		case 95: goto tr19;
		case 110: goto st16;
		case 115: goto tr37;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
tr19:
	{te = p+1;}
	{act = 12;}
	goto st15;
tr37:
	{te = p+1;}
	{act = 7;}
	goto st15;
tr39:
	{te = p+1;}
	{act = 8;}
	goto st15;
tr45:
	{te = p+1;}
	{act = 10;}
	goto st15;
tr48:
	{te = p+1;}
	{act = 4;}
	goto st15;
tr53:
	{te = p+1;}
	{act = 11;}
	goto st15;
tr54:
	{te = p+1;}
	{act = 9;}
	goto st15;
tr58:
	{te = p+1;}
	{act = 5;}
	goto st15;
st15:
	if ( ++p == pe )
		goto _test_eof15;
case 15:
	if ( (*p) == 95 )
		goto tr19;
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr38;
st16:
	if ( ++p == pe )
		goto _test_eof16;
case 16:
	switch( (*p) ) {
		case 68: goto tr39;
		case 95: goto tr19;
		case 100: goto tr39;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
st17:
	if ( ++p == pe )
		goto _test_eof17;
case 17:
	switch( (*p) ) {
		case 69: goto st18;
		case 95: goto tr19;
		case 101: goto st18;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
st18:
	if ( ++p == pe )
		goto _test_eof18;
case 18:
	switch( (*p) ) {
		case 84: goto st19;
		case 95: goto tr19;
		case 116: goto st19;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
st19:
	if ( ++p == pe )
		goto _test_eof19;
case 19:
	switch( (*p) ) {
		case 87: goto st20;
		case 95: goto tr19;
		case 119: goto st20;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
st20:
	if ( ++p == pe )
		goto _test_eof20;
case 20:
	switch( (*p) ) {
		case 69: goto st21;
		case 95: goto tr19;
		case 101: goto st21;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
st21:
	if ( ++p == pe )
		goto _test_eof21;
case 21:
	switch( (*p) ) {
		case 69: goto st22;
		case 95: goto tr19;
		case 101: goto st22;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
st22:
	if ( ++p == pe )
		goto _test_eof22;
case 22:
	switch( (*p) ) {
		case 78: goto tr45;
		case 95: goto tr19;
		case 110: goto tr45;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
st23:
	if ( ++p == pe )
		goto _test_eof23;
case 23:
	switch( (*p) ) {
		case 82: goto st24;
		case 95: goto tr19;
		case 114: goto st24;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
st24:
	if ( ++p == pe )
		goto _test_eof24;
case 24:
	switch( (*p) ) {
		case 79: goto st25;
		case 95: goto tr19;
		case 111: goto st25;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
st25:
	if ( ++p == pe )
		goto _test_eof25;
case 25:
	switch( (*p) ) {
		case 77: goto tr48;
		case 95: goto tr19;
		case 109: goto tr48;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
st26:
	if ( ++p == pe )
		goto _test_eof26;
case 26:
	switch( (*p) ) {
		case 82: goto st27;
		case 95: goto tr19;
		case 114: goto st27;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
st27:
	if ( ++p == pe )
		goto _test_eof27;
case 27:
	switch( (*p) ) {
		case 79: goto st28;
		case 95: goto tr19;
		case 111: goto st28;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
st28:
	if ( ++p == pe )
		goto _test_eof28;
case 28:
	switch( (*p) ) {
		case 85: goto st29;
		case 95: goto tr19;
		case 117: goto st29;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
st29:
	if ( ++p == pe )
		goto _test_eof29;
case 29:
	switch( (*p) ) {
		case 80: goto tr52;
		case 95: goto tr19;
		case 112: goto tr52;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
tr52:
	{te = p+1;}
	goto st30;
st30:
	if ( ++p == pe )
		goto _test_eof30;
case 30:
	switch( (*p) ) {
		case 32: goto st4;
		case 95: goto tr19;
	}
	if ( (*p) < 48 ) {
		if ( 9 <= (*p) && (*p) <= 13 )
			goto st4;
	} else if ( (*p) > 57 ) {
		if ( (*p) > 90 ) {
			if ( 97 <= (*p) && (*p) <= 122 )
				goto tr19;
		} else if ( (*p) >= 65 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
st4:
	if ( ++p == pe )
		goto _test_eof4;
case 4:
	switch( (*p) ) {
		case 32: goto st4;
		case 66: goto st5;
		case 98: goto st5;
	}
	if ( 9 <= (*p) && (*p) <= 13 )
		goto st4;
	goto tr5;
st5:
	if ( ++p == pe )
		goto _test_eof5;
case 5:
	switch( (*p) ) {
		case 89: goto tr8;
		case 121: goto tr8;
	}
	goto tr5;
st31:
	if ( ++p == pe )
		goto _test_eof31;
case 31:
	switch( (*p) ) {
		case 78: goto tr53;
		case 95: goto tr19;
		case 110: goto tr53;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
st32:
	if ( ++p == pe )
		goto _test_eof32;
case 32:
	switch( (*p) ) {
		case 82: goto tr54;
		case 95: goto tr19;
		case 114: goto tr54;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
st33:
	if ( ++p == pe )
		goto _test_eof33;
case 33:
	switch( (*p) ) {
		case 72: goto st34;
		case 95: goto tr19;
		case 104: goto st34;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
st34:
	if ( ++p == pe )
		goto _test_eof34;
case 34:
	switch( (*p) ) {
		case 69: goto st35;
		case 95: goto tr19;
		case 101: goto st35;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
st35:
	if ( ++p == pe )
		goto _test_eof35;
case 35:
	switch( (*p) ) {
		case 82: goto st36;
		case 95: goto tr19;
		case 114: goto st36;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
st36:
	if ( ++p == pe )
		goto _test_eof36;
case 36:
	switch( (*p) ) {
		case 69: goto tr58;
		case 95: goto tr19;
		case 101: goto tr58;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr19;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr19;
	} else
		goto tr19;
	goto tr35;
tr59:
	{te = p+1;}
	goto st37;
tr60:
	{te = p+1;{
            if (++rd == 1) {
                rs = p + 1;
            }
        }}
	goto st37;
tr61:
	cs = 37;
	{te = p+1;{
            if (--rd == 0) {
                re = p;
                type = TToken::YPathLiteral;
                value->build(Context_->Capture(rs, re));
                cs = 6;
                {p++; goto _out;}
            }
        }}
	goto _again;
st37:
	{ts = 0;}
	if ( ++p == pe )
		goto _test_eof37;
case 37:
	{ts = p;}
	switch( (*p) ) {
		case 0: goto st0;
		case 91: goto tr60;
		case 93: goto tr61;
	}
	goto tr59;
	}
	_test_eof6: cs = 6; goto _test_eof; 
	_test_eof7: cs = 7; goto _test_eof; 
	_test_eof1: cs = 1; goto _test_eof; 
	_test_eof8: cs = 8; goto _test_eof; 
	_test_eof9: cs = 9; goto _test_eof; 
	_test_eof2: cs = 2; goto _test_eof; 
	_test_eof3: cs = 3; goto _test_eof; 
	_test_eof10: cs = 10; goto _test_eof; 
	_test_eof11: cs = 11; goto _test_eof; 
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
	_test_eof4: cs = 4; goto _test_eof; 
	_test_eof5: cs = 5; goto _test_eof; 
	_test_eof31: cs = 31; goto _test_eof; 
	_test_eof32: cs = 32; goto _test_eof; 
	_test_eof33: cs = 33; goto _test_eof; 
	_test_eof34: cs = 34; goto _test_eof; 
	_test_eof35: cs = 35; goto _test_eof; 
	_test_eof36: cs = 36; goto _test_eof; 
	_test_eof37: cs = 37; goto _test_eof; 

	_test_eof: {}
	if ( p == eof )
	{
	switch ( cs ) {
	case 7: goto tr27;
	case 8: goto tr28;
	case 9: goto tr30;
	case 2: goto tr2;
	case 3: goto tr2;
	case 10: goto tr30;
	case 11: goto tr32;
	case 12: goto tr28;
	case 13: goto tr28;
	case 14: goto tr35;
	case 15: goto tr38;
	case 16: goto tr35;
	case 17: goto tr35;
	case 18: goto tr35;
	case 19: goto tr35;
	case 20: goto tr35;
	case 21: goto tr35;
	case 22: goto tr35;
	case 23: goto tr35;
	case 24: goto tr35;
	case 25: goto tr35;
	case 26: goto tr35;
	case 27: goto tr35;
	case 28: goto tr35;
	case 29: goto tr35;
	case 30: goto tr35;
	case 4: goto tr5;
	case 5: goto tr5;
	case 31: goto tr35;
	case 32: goto tr35;
	case 33: goto tr35;
	case 34: goto tr35;
	case 35: goto tr35;
	case 36: goto tr35;
	}
	}

	_out: {}
	}

    location->second = p - s;

    if (cs == 
0
) {
        // TODO(sandello): Handle lexer failures.
        return TToken::Failure;
    } else {
        return type;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

