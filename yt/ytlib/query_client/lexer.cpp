
#line 1 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
#include <ytlib/query_client/lexer.h>

#include <util/system/defaults.h>
#include <util/string/cast.h>
#include <util/string/escape.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef TParser::token_type TToken;

////////////////////////////////////////////////////////////////////////////////


#line 119 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"


namespace {

#line 25 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.cpp"
static const int Lexer_start = 6;
static const int Lexer_first_final = 6;
static const int Lexer_error = 0;

static const int Lexer_en_ypath = 30;
static const int Lexer_en_main = 6;


#line 123 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
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

    
#line 59 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.cpp"
	{
	cs = Lexer_start;
	ts = 0;
	te = 0;
	act = 0;
	}

#line 147 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
}

TParser::token_type TLexer::GetNextToken(
    TParser::semantic_type* value,
    TParser::location_type* location
)
{
    TParser::token_type type = TToken::End;

    location->begin = p - s;
    
#line 79 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.cpp"
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
		case 4: goto st4;
		case 5: goto st5;
		case 25: goto st25;
		case 26: goto st26;
		case 27: goto st27;
		case 28: goto st28;
		case 29: goto st29;
		case 30: goto st30;
	default: break;
	}

	if ( ++p == pe )
		goto _test_eof;
_resume:
	switch ( cs )
	{
tr0:
#line 105 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{te = p+1;{ type = TToken::OpNotEqual; {p++; cs = 6; goto _out;} }}
	goto st6;
tr2:
#line 83 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{{p = ((te))-1;}{
            type = TToken::DoubleLiteral;
            value->build(FromString<double>(ts, te - ts));
            {p++; cs = 6; goto _out;}
        }}
	goto st6;
tr5:
#line 73 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{{p = ((te))-1;}{
            type = TToken::Identifier;
            value->build(Context_->Capture(ts, te));
            {p++; cs = 6; goto _out;}
        }}
	goto st6;
tr7:
#line 67 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{te = p+1;{ type = TToken::KwGroupBy;  {p++; cs = 6; goto _out;} }}
	goto st6;
tr8:
#line 113 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{te = p+1;{ type = TToken::End; {p++; cs = 6; goto _out;} }}
	goto st6;
tr11:
#line 108 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{te = p+1;{
            type = static_cast<TToken>((*p));
            {p++; cs = 6; goto _out;}
        }}
	goto st6;
tr22:
#line 95 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{te = p+1;{
            p--;
            {goto st30;}
        }}
	goto st6;
tr23:
#line 99 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{te = p+1;{
            YUNREACHABLE();
        }}
	goto st6;
tr24:
#line 116 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{te = p;p--;{ location->begin = te - s; }}
	goto st6;
tr25:
#line 108 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{te = p;p--;{
            type = static_cast<TToken>((*p));
            {p++; cs = 6; goto _out;}
        }}
	goto st6;
tr27:
#line 83 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{te = p;p--;{
            type = TToken::DoubleLiteral;
            value->build(FromString<double>(ts, te - ts));
            {p++; cs = 6; goto _out;}
        }}
	goto st6;
tr29:
#line 78 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{te = p;p--;{
            type = TToken::IntegerLiteral;
            value->build(FromString<i64>(ts, te - ts));
            {p++; cs = 6; goto _out;}
        }}
	goto st6;
tr30:
#line 103 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{te = p+1;{ type = TToken::OpLessOrEqual; {p++; cs = 6; goto _out;} }}
	goto st6;
tr31:
#line 104 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{te = p+1;{ type = TToken::OpGreaterOrEqual; {p++; cs = 6; goto _out;} }}
	goto st6;
tr32:
#line 73 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{te = p;p--;{
            type = TToken::Identifier;
            value->build(Context_->Capture(ts, te));
            {p++; cs = 6; goto _out;}
        }}
	goto st6;
tr35:
#line 1 "NONE"
	{	switch( act ) {
	case 4:
	{{p = ((te))-1;} type = TToken::KwFrom;   {p++; cs = 6; goto _out;} }
	break;
	case 5:
	{{p = ((te))-1;} type = TToken::KwWhere;  {p++; cs = 6; goto _out;} }
	break;
	case 7:
	{{p = ((te))-1;} type = TToken::KwAs;  {p++; cs = 6; goto _out;} }
	break;
	case 8:
	{{p = ((te))-1;} type = TToken::KwAnd;   {p++; cs = 6; goto _out;} }
	break;
	case 9:
	{{p = ((te))-1;} type = TToken::KwOr;  {p++; cs = 6; goto _out;} }
	break;
	case 10:
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
#line 1 "NONE"
	{ts = 0;}
	if ( ++p == pe )
		goto _test_eof6;
case 6:
#line 1 "NONE"
	{ts = p;}
#line 253 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.cpp"
	switch( (*p) ) {
		case 0: goto tr8;
		case 32: goto st7;
		case 33: goto st1;
		case 37: goto tr11;
		case 46: goto st8;
		case 60: goto st12;
		case 61: goto tr11;
		case 62: goto st13;
		case 65: goto st14;
		case 70: goto st17;
		case 71: goto st20;
		case 79: goto st25;
		case 87: goto st26;
		case 91: goto tr22;
		case 93: goto tr23;
		case 95: goto tr17;
		case 97: goto st14;
		case 102: goto st17;
		case 103: goto st20;
		case 111: goto st25;
		case 119: goto st26;
	}
	if ( (*p) < 48 ) {
		if ( (*p) > 13 ) {
			if ( 40 <= (*p) && (*p) <= 47 )
				goto tr11;
		} else if ( (*p) >= 9 )
			goto st7;
	} else if ( (*p) > 57 ) {
		if ( (*p) > 90 ) {
			if ( 98 <= (*p) && (*p) <= 122 )
				goto tr17;
		} else if ( (*p) >= 66 )
			goto tr17;
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
	goto tr24;
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
		goto tr26;
	goto tr25;
tr26:
#line 1 "NONE"
	{te = p+1;}
	goto st9;
st9:
	if ( ++p == pe )
		goto _test_eof9;
case 9:
#line 326 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.cpp"
	switch( (*p) ) {
		case 69: goto st2;
		case 101: goto st2;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto tr26;
	goto tr27;
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
	goto tr27;
st11:
	if ( ++p == pe )
		goto _test_eof11;
case 11:
	if ( (*p) == 46 )
		goto tr26;
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st11;
	goto tr29;
st12:
	if ( ++p == pe )
		goto _test_eof12;
case 12:
	if ( (*p) == 61 )
		goto tr30;
	goto tr25;
st13:
	if ( ++p == pe )
		goto _test_eof13;
case 13:
	if ( (*p) == 61 )
		goto tr31;
	goto tr25;
st14:
	if ( ++p == pe )
		goto _test_eof14;
case 14:
	switch( (*p) ) {
		case 78: goto st16;
		case 83: goto tr34;
		case 95: goto tr17;
		case 110: goto st16;
		case 115: goto tr34;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr17;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr17;
	} else
		goto tr17;
	goto tr32;
tr17:
#line 1 "NONE"
	{te = p+1;}
#line 73 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{act = 10;}
	goto st15;
tr34:
#line 1 "NONE"
	{te = p+1;}
#line 68 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{act = 7;}
	goto st15;
tr36:
#line 1 "NONE"
	{te = p+1;}
#line 70 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{act = 8;}
	goto st15;
tr39:
#line 1 "NONE"
	{te = p+1;}
#line 65 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{act = 4;}
	goto st15;
tr45:
#line 1 "NONE"
	{te = p+1;}
#line 71 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{act = 9;}
	goto st15;
tr49:
#line 1 "NONE"
	{te = p+1;}
#line 66 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{act = 5;}
	goto st15;
st15:
	if ( ++p == pe )
		goto _test_eof15;
case 15:
#line 442 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.cpp"
	if ( (*p) == 95 )
		goto tr17;
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr17;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr17;
	} else
		goto tr17;
	goto tr35;
st16:
	if ( ++p == pe )
		goto _test_eof16;
case 16:
	switch( (*p) ) {
		case 68: goto tr36;
		case 95: goto tr17;
		case 100: goto tr36;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr17;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr17;
	} else
		goto tr17;
	goto tr32;
st17:
	if ( ++p == pe )
		goto _test_eof17;
case 17:
	switch( (*p) ) {
		case 82: goto st18;
		case 95: goto tr17;
		case 114: goto st18;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr17;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr17;
	} else
		goto tr17;
	goto tr32;
st18:
	if ( ++p == pe )
		goto _test_eof18;
case 18:
	switch( (*p) ) {
		case 79: goto st19;
		case 95: goto tr17;
		case 111: goto st19;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr17;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr17;
	} else
		goto tr17;
	goto tr32;
st19:
	if ( ++p == pe )
		goto _test_eof19;
case 19:
	switch( (*p) ) {
		case 77: goto tr39;
		case 95: goto tr17;
		case 109: goto tr39;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr17;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr17;
	} else
		goto tr17;
	goto tr32;
st20:
	if ( ++p == pe )
		goto _test_eof20;
case 20:
	switch( (*p) ) {
		case 82: goto st21;
		case 95: goto tr17;
		case 114: goto st21;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr17;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr17;
	} else
		goto tr17;
	goto tr32;
st21:
	if ( ++p == pe )
		goto _test_eof21;
case 21:
	switch( (*p) ) {
		case 79: goto st22;
		case 95: goto tr17;
		case 111: goto st22;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr17;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr17;
	} else
		goto tr17;
	goto tr32;
st22:
	if ( ++p == pe )
		goto _test_eof22;
case 22:
	switch( (*p) ) {
		case 85: goto st23;
		case 95: goto tr17;
		case 117: goto st23;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr17;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr17;
	} else
		goto tr17;
	goto tr32;
st23:
	if ( ++p == pe )
		goto _test_eof23;
case 23:
	switch( (*p) ) {
		case 80: goto tr43;
		case 95: goto tr17;
		case 112: goto tr43;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr17;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr17;
	} else
		goto tr17;
	goto tr32;
tr43:
#line 1 "NONE"
	{te = p+1;}
	goto st24;
st24:
	if ( ++p == pe )
		goto _test_eof24;
case 24:
#line 606 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.cpp"
	switch( (*p) ) {
		case 32: goto st4;
		case 95: goto tr17;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr17;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr17;
	} else
		goto tr17;
	goto tr32;
st4:
	if ( ++p == pe )
		goto _test_eof4;
case 4:
	switch( (*p) ) {
		case 66: goto st5;
		case 98: goto st5;
	}
	goto tr5;
st5:
	if ( ++p == pe )
		goto _test_eof5;
case 5:
	switch( (*p) ) {
		case 89: goto tr7;
		case 121: goto tr7;
	}
	goto tr5;
st25:
	if ( ++p == pe )
		goto _test_eof25;
case 25:
	switch( (*p) ) {
		case 82: goto tr45;
		case 95: goto tr17;
		case 114: goto tr45;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr17;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr17;
	} else
		goto tr17;
	goto tr32;
st26:
	if ( ++p == pe )
		goto _test_eof26;
case 26:
	switch( (*p) ) {
		case 72: goto st27;
		case 95: goto tr17;
		case 104: goto st27;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr17;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr17;
	} else
		goto tr17;
	goto tr32;
st27:
	if ( ++p == pe )
		goto _test_eof27;
case 27:
	switch( (*p) ) {
		case 69: goto st28;
		case 95: goto tr17;
		case 101: goto st28;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr17;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr17;
	} else
		goto tr17;
	goto tr32;
st28:
	if ( ++p == pe )
		goto _test_eof28;
case 28:
	switch( (*p) ) {
		case 82: goto st29;
		case 95: goto tr17;
		case 114: goto st29;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr17;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr17;
	} else
		goto tr17;
	goto tr32;
st29:
	if ( ++p == pe )
		goto _test_eof29;
case 29:
	switch( (*p) ) {
		case 69: goto tr49;
		case 95: goto tr17;
		case 101: goto tr49;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr17;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr17;
	} else
		goto tr17;
	goto tr32;
tr50:
#line 60 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{te = p+1;}
	goto st30;
tr51:
#line 46 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
	{te = p+1;{
            if (++rd == 1) {
                rs = p + 1;
            }
        }}
	goto st30;
tr52:
	cs = 30;
#line 51 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
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
st30:
#line 1 "NONE"
	{ts = 0;}
	if ( ++p == pe )
		goto _test_eof30;
case 30:
#line 1 "NONE"
	{ts = p;}
#line 761 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.cpp"
	switch( (*p) ) {
		case 0: goto st0;
		case 91: goto tr51;
		case 93: goto tr52;
	}
	goto tr50;
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
	_test_eof4: cs = 4; goto _test_eof; 
	_test_eof5: cs = 5; goto _test_eof; 
	_test_eof25: cs = 25; goto _test_eof; 
	_test_eof26: cs = 26; goto _test_eof; 
	_test_eof27: cs = 27; goto _test_eof; 
	_test_eof28: cs = 28; goto _test_eof; 
	_test_eof29: cs = 29; goto _test_eof; 
	_test_eof30: cs = 30; goto _test_eof; 

	_test_eof: {}
	if ( p == eof )
	{
	switch ( cs ) {
	case 7: goto tr24;
	case 8: goto tr25;
	case 9: goto tr27;
	case 2: goto tr2;
	case 3: goto tr2;
	case 10: goto tr27;
	case 11: goto tr29;
	case 12: goto tr25;
	case 13: goto tr25;
	case 14: goto tr32;
	case 15: goto tr35;
	case 16: goto tr32;
	case 17: goto tr32;
	case 18: goto tr32;
	case 19: goto tr32;
	case 20: goto tr32;
	case 21: goto tr32;
	case 22: goto tr32;
	case 23: goto tr32;
	case 24: goto tr32;
	case 4: goto tr5;
	case 5: goto tr5;
	case 25: goto tr32;
	case 26: goto tr32;
	case 27: goto tr32;
	case 28: goto tr32;
	case 29: goto tr32;
	}
	}

	_out: {}
	}

#line 158 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
    location->end = p - s;

    if (cs == 
#line 841 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.cpp"
0
#line 160 "/home/lukyan/dev/yt/yt/ytlib/query_client/lexer.rl"
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

