
#line 1 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
#include <ytlib/query_client/lexer.h>

#include <util/system/defaults.h>
#include <util/string/cast.h>
#include <util/string/escape.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef TParser::token_type TToken;

////////////////////////////////////////////////////////////////////////////////


#line 108 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"


namespace {

#line 25 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.cpp"
static const int Lexer_start = 5;
static const int Lexer_first_final = 5;
static const int Lexer_error = 0;

static const int Lexer_en_ypath = 20;
static const int Lexer_en_main = 5;


#line 112 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
} // namespace anonymous

TLexer::TLexer(TQueryContext* context, const Stroka& source)
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

    
#line 59 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.cpp"
	{
	cs = Lexer_start;
	ts = 0;
	te = 0;
	act = 0;
	}

#line 136 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
}

TParser::token_type TLexer::GetNextToken(
    TParser::semantic_type* value,
    TParser::location_type* location
)
{
    TParser::token_type type = TToken::End;

    location->begin = p - s;
    
#line 79 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.cpp"
	{
	if ( p == pe )
		goto _test_eof;
	goto _resume;

_again:
	switch ( cs ) {
		case 5: goto st5;
		case 0: goto st0;
		case 6: goto st6;
		case 1: goto st1;
		case 2: goto st2;
		case 7: goto st7;
		case 3: goto st3;
		case 4: goto st4;
		case 8: goto st8;
		case 9: goto st9;
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
	default: break;
	}

	if ( ++p == pe )
		goto _test_eof;
_resume:
	switch ( cs )
	{
tr0:
#line 94 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
	{te = p+1;{ type = TToken::OpNotEqual; {p++; cs = 5; goto _out;} }}
	goto st5;
tr3:
#line 72 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
	{{p = ((te))-1;}{
            type = TToken::DoubleLiteral;
            value->build(FromString<double>(ts, te - ts));
            {p++; cs = 5; goto _out;}
        }}
	goto st5;
tr6:
#line 102 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
	{te = p+1;{ type = TToken::End; {p++; cs = 5; goto _out;} }}
	goto st5;
tr9:
#line 97 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
	{te = p+1;{
            type = static_cast<TToken>((*p));
            {p++; cs = 5; goto _out;}
        }}
	goto st5;
tr17:
#line 84 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
	{te = p+1;{
            p--;
            {goto st20;}
        }}
	goto st5;
tr18:
#line 88 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
	{te = p+1;{
            YUNREACHABLE();
        }}
	goto st5;
tr19:
#line 105 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
	{te = p;p--;{ location->begin = te - s; }}
	goto st5;
tr20:
#line 72 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
	{te = p;p--;{
            type = TToken::DoubleLiteral;
            value->build(FromString<double>(ts, te - ts));
            {p++; cs = 5; goto _out;}
        }}
	goto st5;
tr22:
#line 67 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
	{te = p;p--;{
            type = TToken::IntegerLiteral;
            value->build(FromString<i64>(ts, te - ts));
            {p++; cs = 5; goto _out;}
        }}
	goto st5;
tr23:
#line 97 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
	{te = p;p--;{
            type = static_cast<TToken>((*p));
            {p++; cs = 5; goto _out;}
        }}
	goto st5;
tr24:
#line 92 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
	{te = p+1;{ type = TToken::OpLessOrEqual; {p++; cs = 5; goto _out;} }}
	goto st5;
tr25:
#line 93 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
	{te = p+1;{ type = TToken::OpGreaterOrEqual; {p++; cs = 5; goto _out;} }}
	goto st5;
tr26:
#line 1 "NONE"
	{	switch( act ) {
	case 4:
	{{p = ((te))-1;} type = TToken::KwFrom;   {p++; cs = 5; goto _out;} }
	break;
	case 5:
	{{p = ((te))-1;} type = TToken::KwWhere;  {p++; cs = 5; goto _out;} }
	break;
	case 6:
	{{p = ((te))-1;}
            type = TToken::Identifier;
            value->build(Context_->Capture(ts, te));
            {p++; cs = 5; goto _out;}
        }
	break;
	}
	}
	goto st5;
tr27:
#line 62 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
	{te = p;p--;{
            type = TToken::Identifier;
            value->build(Context_->Capture(ts, te));
            {p++; cs = 5; goto _out;}
        }}
	goto st5;
st5:
#line 1 "NONE"
	{ts = 0;}
	if ( ++p == pe )
		goto _test_eof5;
case 5:
#line 1 "NONE"
	{ts = p;}
#line 222 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.cpp"
	switch( (*p) ) {
		case 0: goto tr6;
		case 32: goto st6;
		case 33: goto st1;
		case 44: goto tr9;
		case 46: goto st2;
		case 60: goto st10;
		case 61: goto tr9;
		case 62: goto st11;
		case 70: goto st13;
		case 87: goto st16;
		case 91: goto tr17;
		case 93: goto tr18;
		case 95: goto tr14;
		case 102: goto st13;
		case 119: goto st16;
	}
	if ( (*p) < 48 ) {
		if ( (*p) > 13 ) {
			if ( 40 <= (*p) && (*p) <= 42 )
				goto tr9;
		} else if ( (*p) >= 9 )
			goto st6;
	} else if ( (*p) > 57 ) {
		if ( (*p) > 90 ) {
			if ( 97 <= (*p) && (*p) <= 122 )
				goto tr14;
		} else if ( (*p) >= 65 )
			goto tr14;
	} else
		goto st9;
	goto st0;
st0:
cs = 0;
	goto _out;
st6:
	if ( ++p == pe )
		goto _test_eof6;
case 6:
	if ( (*p) == 32 )
		goto st6;
	if ( 9 <= (*p) && (*p) <= 13 )
		goto st6;
	goto tr19;
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
	if ( 48 <= (*p) && (*p) <= 57 )
		goto tr2;
	goto st0;
tr2:
#line 1 "NONE"
	{te = p+1;}
	goto st7;
st7:
	if ( ++p == pe )
		goto _test_eof7;
case 7:
#line 289 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.cpp"
	switch( (*p) ) {
		case 69: goto st3;
		case 101: goto st3;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto tr2;
	goto tr20;
st3:
	if ( ++p == pe )
		goto _test_eof3;
case 3:
	switch( (*p) ) {
		case 43: goto st4;
		case 45: goto st4;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st8;
	goto tr3;
st4:
	if ( ++p == pe )
		goto _test_eof4;
case 4:
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st8;
	goto tr3;
st8:
	if ( ++p == pe )
		goto _test_eof8;
case 8:
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st8;
	goto tr20;
st9:
	if ( ++p == pe )
		goto _test_eof9;
case 9:
	if ( (*p) == 46 )
		goto tr2;
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st9;
	goto tr22;
st10:
	if ( ++p == pe )
		goto _test_eof10;
case 10:
	if ( (*p) == 61 )
		goto tr24;
	goto tr23;
st11:
	if ( ++p == pe )
		goto _test_eof11;
case 11:
	if ( (*p) == 61 )
		goto tr25;
	goto tr23;
tr14:
#line 1 "NONE"
	{te = p+1;}
#line 62 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
	{act = 6;}
	goto st12;
tr30:
#line 1 "NONE"
	{te = p+1;}
#line 59 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
	{act = 4;}
	goto st12;
tr34:
#line 1 "NONE"
	{te = p+1;}
#line 60 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
	{act = 5;}
	goto st12;
st12:
	if ( ++p == pe )
		goto _test_eof12;
case 12:
#line 367 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.cpp"
	if ( (*p) == 95 )
		goto tr14;
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr14;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr14;
	} else
		goto tr14;
	goto tr26;
st13:
	if ( ++p == pe )
		goto _test_eof13;
case 13:
	switch( (*p) ) {
		case 82: goto st14;
		case 95: goto tr14;
		case 114: goto st14;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr14;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr14;
	} else
		goto tr14;
	goto tr27;
st14:
	if ( ++p == pe )
		goto _test_eof14;
case 14:
	switch( (*p) ) {
		case 79: goto st15;
		case 95: goto tr14;
		case 111: goto st15;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr14;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr14;
	} else
		goto tr14;
	goto tr27;
st15:
	if ( ++p == pe )
		goto _test_eof15;
case 15:
	switch( (*p) ) {
		case 77: goto tr30;
		case 95: goto tr14;
		case 109: goto tr30;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr14;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr14;
	} else
		goto tr14;
	goto tr27;
st16:
	if ( ++p == pe )
		goto _test_eof16;
case 16:
	switch( (*p) ) {
		case 72: goto st17;
		case 95: goto tr14;
		case 104: goto st17;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr14;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr14;
	} else
		goto tr14;
	goto tr27;
st17:
	if ( ++p == pe )
		goto _test_eof17;
case 17:
	switch( (*p) ) {
		case 69: goto st18;
		case 95: goto tr14;
		case 101: goto st18;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr14;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr14;
	} else
		goto tr14;
	goto tr27;
st18:
	if ( ++p == pe )
		goto _test_eof18;
case 18:
	switch( (*p) ) {
		case 82: goto st19;
		case 95: goto tr14;
		case 114: goto st19;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr14;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr14;
	} else
		goto tr14;
	goto tr27;
st19:
	if ( ++p == pe )
		goto _test_eof19;
case 19:
	switch( (*p) ) {
		case 69: goto tr34;
		case 95: goto tr14;
		case 101: goto tr34;
	}
	if ( (*p) < 65 ) {
		if ( 48 <= (*p) && (*p) <= 57 )
			goto tr14;
	} else if ( (*p) > 90 ) {
		if ( 97 <= (*p) && (*p) <= 122 )
			goto tr14;
	} else
		goto tr14;
	goto tr27;
tr35:
#line 54 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
	{te = p+1;}
	goto st20;
tr36:
#line 40 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
	{te = p+1;{
            if (++rd == 1) {
                rs = p + 1;
            }
        }}
	goto st20;
tr37:
	cs = 20;
#line 45 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
	{te = p+1;{
            if (--rd == 0) {
                re = p;
                type = TToken::YPathLiteral;
                value->build(Context_->Capture(rs, re));
                cs = 5;
                {p++; goto _out;}
            }
        }}
	goto _again;
st20:
#line 1 "NONE"
	{ts = 0;}
	if ( ++p == pe )
		goto _test_eof20;
case 20:
#line 1 "NONE"
	{ts = p;}
#line 538 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.cpp"
	switch( (*p) ) {
		case 0: goto st0;
		case 91: goto tr36;
		case 93: goto tr37;
	}
	goto tr35;
	}
	_test_eof5: cs = 5; goto _test_eof; 
	_test_eof6: cs = 6; goto _test_eof; 
	_test_eof1: cs = 1; goto _test_eof; 
	_test_eof2: cs = 2; goto _test_eof; 
	_test_eof7: cs = 7; goto _test_eof; 
	_test_eof3: cs = 3; goto _test_eof; 
	_test_eof4: cs = 4; goto _test_eof; 
	_test_eof8: cs = 8; goto _test_eof; 
	_test_eof9: cs = 9; goto _test_eof; 
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

	_test_eof: {}
	if ( p == eof )
	{
	switch ( cs ) {
	case 6: goto tr19;
	case 7: goto tr20;
	case 3: goto tr3;
	case 4: goto tr3;
	case 8: goto tr20;
	case 9: goto tr22;
	case 10: goto tr23;
	case 11: goto tr23;
	case 12: goto tr26;
	case 13: goto tr27;
	case 14: goto tr27;
	case 15: goto tr27;
	case 16: goto tr27;
	case 17: goto tr27;
	case 18: goto tr27;
	case 19: goto tr27;
	}
	}

	_out: {}
	}

#line 147 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
    location->end = p - s;

    if (cs == 
#line 597 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.cpp"
0
#line 149 "/home/sandello/yt/source/yt/ytlib/query_client/lexer.rl"
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

