
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




namespace {

static const int Lexer_start = 25;
static const int Lexer_first_final = 25;
static const int Lexer_error = 0;

static const int Lexer_en_quoted_identifier = 102;
static const int Lexer_en_main = 25;


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
  case 25: goto st25;
  case 0: goto st0;
  case 26: goto st26;
  case 1: goto st1;
  case 2: goto st2;
  case 3: goto st3;
  case 27: goto st27;
  case 4: goto st4;
  case 5: goto st5;
  case 6: goto st6;
  case 7: goto st7;
  case 8: goto st8;
  case 9: goto st9;
  case 10: goto st10;
  case 11: goto st11;
  case 12: goto st12;
  case 28: goto st28;
  case 29: goto st29;
  case 13: goto st13;
  case 14: goto st14;
  case 30: goto st30;
  case 31: goto st31;
  case 32: goto st32;
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
  case 44: goto st44;
  case 45: goto st45;
  case 46: goto st46;
  case 47: goto st47;
  case 48: goto st48;
  case 49: goto st49;
  case 50: goto st50;
  case 51: goto st51;
  case 52: goto st52;
  case 53: goto st53;
  case 54: goto st54;
  case 55: goto st55;
  case 56: goto st56;
  case 57: goto st57;
  case 15: goto st15;
  case 16: goto st16;
  case 58: goto st58;
  case 59: goto st59;
  case 60: goto st60;
  case 61: goto st61;
  case 62: goto st62;
  case 63: goto st63;
  case 64: goto st64;
  case 65: goto st65;
  case 66: goto st66;
  case 67: goto st67;
  case 68: goto st68;
  case 69: goto st69;
  case 70: goto st70;
  case 71: goto st71;
  case 72: goto st72;
  case 73: goto st73;
  case 74: goto st74;
  case 75: goto st75;
  case 76: goto st76;
  case 77: goto st77;
  case 78: goto st78;
  case 79: goto st79;
  case 80: goto st80;
  case 81: goto st81;
  case 17: goto st17;
  case 18: goto st18;
  case 82: goto st82;
  case 83: goto st83;
  case 84: goto st84;
  case 85: goto st85;
  case 86: goto st86;
  case 87: goto st87;
  case 88: goto st88;
  case 89: goto st89;
  case 90: goto st90;
  case 91: goto st91;
  case 92: goto st92;
  case 93: goto st93;
  case 94: goto st94;
  case 95: goto st95;
  case 96: goto st96;
  case 97: goto st97;
  case 98: goto st98;
  case 99: goto st99;
  case 100: goto st100;
  case 101: goto st101;
  case 19: goto st19;
  case 20: goto st20;
  case 21: goto st21;
  case 22: goto st22;
  case 23: goto st23;
  case 24: goto st24;
  case 102: goto st102;
 default: break;
 }

 if ( ++p == pe )
  goto _test_eof;
_resume:
 switch ( cs )
 {
tr0:
 {te = p+1;{ type = TToken::OpNotEqual; {p++; cs = 25; goto _out;} }}
 goto st25;
tr3:
 {te = p+1;{
            type = TToken::StringLiteral;
            value->build(UnescapeC(ts + 1, te - ts - 2));
            {p++; cs = 25; goto _out;}
        }}
 goto st25;
tr5:
 {{p = ((te))-1;}{
            type = static_cast<TToken>((*p));
            {p++; cs = 25; goto _out;}
        }}
 goto st25;
tr9:
 {te = p+1;{ type = TToken::KwFalse; {p++; cs = 25; goto _out;} }}
 goto st25;
tr12:
 {te = p+1;{ type = TToken::KwTrue; {p++; cs = 25; goto _out;} }}
 goto st25;
tr15:
 {{p = ((te))-1;}{
            type = TToken::DoubleLiteral;
            value->build(FromString<double>(ts, te - ts));
            {p++; cs = 25; goto _out;}
        }}
 goto st25;
tr18:
 {{p = ((te))-1;}{
            type = TToken::Identifier;
            value->build(TStringBuf(ts, te));
            {p++; cs = 25; goto _out;}
        }}
 goto st25;
tr21:
 {te = p+1;{ type = TToken::KwGroupBy; {p++; cs = 25; goto _out;} }}
 goto st25;
tr24:
 {te = p+1;{ type = TToken::KwOrderBy; {p++; cs = 25; goto _out;} }}
 goto st25;
tr31:
 {te = p+1;{ type = TToken::KwWithTotals; {p++; cs = 25; goto _out;} }}
 goto st25;
tr32:
 {te = p+1;{ type = TToken::End; {p++; cs = 25; goto _out;} }}
 goto st25;
tr35:
 {te = p+1;{
            type = static_cast<TToken>((*p));
            {p++; cs = 25; goto _out;}
        }}
 goto st25;
tr56:
 {te = p+1;{
            p--;
            {goto st102;}
        }}
 goto st25;
tr57:
 {te = p+1;{
            THROW_ERROR_EXCEPTION("Unexpected symbol \"]\" at position %v", ts - p);
        }}
 goto st25;
tr58:
 {te = p;p--;{ location->first = te - s; }}
 goto st25;
tr59:
 {te = p;p--;{
            type = static_cast<TToken>((*p));
            {p++; cs = 25; goto _out;}
        }}
 goto st25;
tr63:
 {te = p;p--;{
            type = TToken::DoubleLiteral;
            value->build(FromString<double>(ts, te - ts));
            {p++; cs = 25; goto _out;}
        }}
 goto st25;
tr65:
 {te = p;p--;{
            type = TToken::Int64Literal;
            value->build(FromString<ui64>(ts, te - ts));
            {p++; cs = 25; goto _out;}
        }}
 goto st25;
tr66:
 {te = p+1;{
            type = TToken::Uint64Literal;
            value->build(FromString<ui64>(ts, te - ts - 1));
            {p++; cs = 25; goto _out;}
        }}
 goto st25;
tr67:
 {te = p+1;{ type = TToken::OpLeftShift; {p++; cs = 25; goto _out;} }}
 goto st25;
tr68:
 {te = p+1;{ type = TToken::OpLessOrEqual; {p++; cs = 25; goto _out;} }}
 goto st25;
tr69:
 {te = p+1;{ type = TToken::OpGreaterOrEqual; {p++; cs = 25; goto _out;} }}
 goto st25;
tr70:
 {te = p+1;{ type = TToken::OpRightShift; {p++; cs = 25; goto _out;} }}
 goto st25;
tr71:
 {te = p;p--;{
            type = TToken::Identifier;
            value->build(TStringBuf(ts, te));
            {p++; cs = 25; goto _out;}
        }}
 goto st25;
tr74:
 { switch( act ) {
 case 4:
 {{p = ((te))-1;} type = TToken::KwFrom; {p++; cs = 25; goto _out;} }
 break;
 case 5:
 {{p = ((te))-1;} type = TToken::KwWhere; {p++; cs = 25; goto _out;} }
 break;
 case 6:
 {{p = ((te))-1;} type = TToken::KwHaving; {p++; cs = 25; goto _out;} }
 break;
 case 7:
 {{p = ((te))-1;} type = TToken::KwLimit; {p++; cs = 25; goto _out;} }
 break;
 case 8:
 {{p = ((te))-1;} type = TToken::KwJoin; {p++; cs = 25; goto _out;} }
 break;
 case 9:
 {{p = ((te))-1;} type = TToken::KwUsing; {p++; cs = 25; goto _out;} }
 break;
 case 13:
 {{p = ((te))-1;} type = TToken::KwAsc; {p++; cs = 25; goto _out;} }
 break;
 case 14:
 {{p = ((te))-1;} type = TToken::KwDesc; {p++; cs = 25; goto _out;} }
 break;
 case 15:
 {{p = ((te))-1;} type = TToken::KwLeft; {p++; cs = 25; goto _out;} }
 break;
 case 17:
 {{p = ((te))-1;} type = TToken::KwOn; {p++; cs = 25; goto _out;} }
 break;
 case 18:
 {{p = ((te))-1;} type = TToken::KwAnd; {p++; cs = 25; goto _out;} }
 break;
 case 20:
 {{p = ((te))-1;} type = TToken::KwNot; {p++; cs = 25; goto _out;} }
 break;
 case 21:
 {{p = ((te))-1;} type = TToken::KwNull; {p++; cs = 25; goto _out;} }
 break;
 case 22:
 {{p = ((te))-1;} type = TToken::KwBetween; {p++; cs = 25; goto _out;} }
 break;
 case 23:
 {{p = ((te))-1;} type = TToken::KwIn; {p++; cs = 25; goto _out;} }
 break;
 case 24:
 {{p = ((te))-1;} type = TToken::KwTransform; {p++; cs = 25; goto _out;} }
 break;
 case 25:
 {{p = ((te))-1;} type = TToken::KwFalse; {p++; cs = 25; goto _out;} }
 break;
 case 26:
 {{p = ((te))-1;} type = TToken::KwTrue; {p++; cs = 25; goto _out;} }
 break;
 case 29:
 {{p = ((te))-1;}
            type = TToken::Identifier;
            value->build(TStringBuf(ts, te));
            {p++; cs = 25; goto _out;}
        }
 break;
 }
 }
 goto st25;
tr76:
 {te = p;p--;{ type = TToken::KwAs; {p++; cs = 25; goto _out;} }}
 goto st25;
tr121:
 {te = p;p--;{ type = TToken::KwOr; {p++; cs = 25; goto _out;} }}
 goto st25;
st25:
 {ts = 0;}
 if ( ++p == pe )
  goto _test_eof25;
case 25:
 {ts = p;}
 switch( (*p) ) {
  case 0: goto tr32;
  case 32: goto st26;
  case 33: goto st1;
  case 34: goto st2;
  case 35: goto tr35;
  case 37: goto tr36;
  case 39: goto st11;
  case 46: goto st28;
  case 60: goto st32;
  case 61: goto tr35;
  case 62: goto st33;
  case 65: goto st34;
  case 66: goto st38;
  case 68: goto st44;
  case 70: goto st47;
  case 71: goto st53;
  case 72: goto st58;
  case 73: goto st63;
  case 74: goto st64;
  case 76: goto st67;
  case 78: goto st73;
  case 79: goto st77;
  case 84: goto st82;
  case 85: goto st91;
  case 87: goto st95;
  case 91: goto tr56;
  case 93: goto tr57;
  case 95: goto tr43;
  case 97: goto st34;
  case 98: goto st38;
  case 100: goto st44;
  case 102: goto st47;
  case 103: goto st53;
  case 104: goto st58;
  case 105: goto st63;
  case 106: goto st64;
  case 108: goto st67;
  case 110: goto st73;
  case 111: goto st77;
  case 116: goto st82;
  case 117: goto st91;
  case 119: goto st95;
  case 124: goto tr35;
  case 126: goto tr35;
 }
 if ( (*p) < 48 ) {
  if ( (*p) > 13 ) {
   if ( 38 <= (*p) && (*p) <= 47 )
    goto tr35;
  } else if ( (*p) >= 9 )
   goto st26;
 } else if ( (*p) > 57 ) {
  if ( (*p) > 90 ) {
   if ( 99 <= (*p) && (*p) <= 122 )
    goto tr43;
  } else if ( (*p) >= 67 )
   goto tr43;
 } else
  goto st31;
 goto st0;
st0:
cs = 0;
 goto _out;
st26:
 if ( ++p == pe )
  goto _test_eof26;
case 26:
 if ( (*p) == 32 )
  goto st26;
 if ( 9 <= (*p) && (*p) <= 13 )
  goto st26;
 goto tr58;
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
tr36:
 {te = p+1;}
 goto st27;
st27:
 if ( ++p == pe )
  goto _test_eof27;
case 27:
 switch( (*p) ) {
  case 70: goto st4;
  case 84: goto st8;
  case 102: goto st4;
  case 116: goto st8;
 }
 goto tr59;
st4:
 if ( ++p == pe )
  goto _test_eof4;
case 4:
 switch( (*p) ) {
  case 65: goto st5;
  case 97: goto st5;
 }
 goto tr5;
st5:
 if ( ++p == pe )
  goto _test_eof5;
case 5:
 switch( (*p) ) {
  case 76: goto st6;
  case 108: goto st6;
 }
 goto tr5;
st6:
 if ( ++p == pe )
  goto _test_eof6;
case 6:
 switch( (*p) ) {
  case 83: goto st7;
  case 115: goto st7;
 }
 goto tr5;
st7:
 if ( ++p == pe )
  goto _test_eof7;
case 7:
 switch( (*p) ) {
  case 69: goto tr9;
  case 101: goto tr9;
 }
 goto tr5;
st8:
 if ( ++p == pe )
  goto _test_eof8;
case 8:
 switch( (*p) ) {
  case 82: goto st9;
  case 114: goto st9;
 }
 goto tr5;
st9:
 if ( ++p == pe )
  goto _test_eof9;
case 9:
 switch( (*p) ) {
  case 85: goto st10;
  case 117: goto st10;
 }
 goto tr5;
st10:
 if ( ++p == pe )
  goto _test_eof10;
case 10:
 switch( (*p) ) {
  case 69: goto tr12;
  case 101: goto tr12;
 }
 goto tr5;
st11:
 if ( ++p == pe )
  goto _test_eof11;
case 11:
 switch( (*p) ) {
  case 39: goto tr3;
  case 92: goto st12;
 }
 goto st11;
st12:
 if ( ++p == pe )
  goto _test_eof12;
case 12:
 goto st11;
st28:
 if ( ++p == pe )
  goto _test_eof28;
case 28:
 if ( 48 <= (*p) && (*p) <= 57 )
  goto tr62;
 goto tr59;
tr62:
 {te = p+1;}
 goto st29;
st29:
 if ( ++p == pe )
  goto _test_eof29;
case 29:
 switch( (*p) ) {
  case 69: goto st13;
  case 101: goto st13;
 }
 if ( 48 <= (*p) && (*p) <= 57 )
  goto tr62;
 goto tr63;
st13:
 if ( ++p == pe )
  goto _test_eof13;
case 13:
 switch( (*p) ) {
  case 43: goto st14;
  case 45: goto st14;
 }
 if ( 48 <= (*p) && (*p) <= 57 )
  goto st30;
 goto tr15;
st14:
 if ( ++p == pe )
  goto _test_eof14;
case 14:
 if ( 48 <= (*p) && (*p) <= 57 )
  goto st30;
 goto tr15;
st30:
 if ( ++p == pe )
  goto _test_eof30;
case 30:
 if ( 48 <= (*p) && (*p) <= 57 )
  goto st30;
 goto tr63;
st31:
 if ( ++p == pe )
  goto _test_eof31;
case 31:
 switch( (*p) ) {
  case 46: goto tr62;
  case 117: goto tr66;
 }
 if ( 48 <= (*p) && (*p) <= 57 )
  goto st31;
 goto tr65;
st32:
 if ( ++p == pe )
  goto _test_eof32;
case 32:
 switch( (*p) ) {
  case 60: goto tr67;
  case 61: goto tr68;
 }
 goto tr59;
st33:
 if ( ++p == pe )
  goto _test_eof33;
case 33:
 switch( (*p) ) {
  case 61: goto tr69;
  case 62: goto tr70;
 }
 goto tr59;
st34:
 if ( ++p == pe )
  goto _test_eof34;
case 34:
 switch( (*p) ) {
  case 78: goto st36;
  case 83: goto st37;
  case 95: goto tr43;
  case 110: goto st36;
  case 115: goto st37;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
tr43:
 {te = p+1;}
 {act = 29;}
 goto st35;
tr75:
 {te = p+1;}
 {act = 18;}
 goto st35;
tr77:
 {te = p+1;}
 {act = 13;}
 goto st35;
tr83:
 {te = p+1;}
 {act = 22;}
 goto st35;
tr86:
 {te = p+1;}
 {act = 14;}
 goto st35;
tr91:
 {te = p+1;}
 {act = 25;}
 goto st35;
tr93:
 {te = p+1;}
 {act = 4;}
 goto st35;
tr102:
 {te = p+1;}
 {act = 6;}
 goto st35;
tr103:
 {te = p+1;}
 {act = 23;}
 goto st35;
tr106:
 {te = p+1;}
 {act = 8;}
 goto st35;
tr110:
 {te = p+1;}
 {act = 15;}
 goto st35;
tr113:
 {te = p+1;}
 {act = 7;}
 goto st35;
tr116:
 {te = p+1;}
 {act = 20;}
 goto st35;
tr118:
 {te = p+1;}
 {act = 21;}
 goto st35;
tr119:
 {te = p+1;}
 {act = 17;}
 goto st35;
tr133:
 {te = p+1;}
 {act = 24;}
 goto st35;
tr134:
 {te = p+1;}
 {act = 26;}
 goto st35;
tr138:
 {te = p+1;}
 {act = 9;}
 goto st35;
tr143:
 {te = p+1;}
 {act = 5;}
 goto st35;
st35:
 if ( ++p == pe )
  goto _test_eof35;
case 35:
 if ( (*p) == 95 )
  goto tr43;
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr74;
st36:
 if ( ++p == pe )
  goto _test_eof36;
case 36:
 switch( (*p) ) {
  case 68: goto tr75;
  case 95: goto tr43;
  case 100: goto tr75;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st37:
 if ( ++p == pe )
  goto _test_eof37;
case 37:
 switch( (*p) ) {
  case 67: goto tr77;
  case 95: goto tr43;
  case 99: goto tr77;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr76;
st38:
 if ( ++p == pe )
  goto _test_eof38;
case 38:
 switch( (*p) ) {
  case 69: goto st39;
  case 95: goto tr43;
  case 101: goto st39;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st39:
 if ( ++p == pe )
  goto _test_eof39;
case 39:
 switch( (*p) ) {
  case 84: goto st40;
  case 95: goto tr43;
  case 116: goto st40;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st40:
 if ( ++p == pe )
  goto _test_eof40;
case 40:
 switch( (*p) ) {
  case 87: goto st41;
  case 95: goto tr43;
  case 119: goto st41;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st41:
 if ( ++p == pe )
  goto _test_eof41;
case 41:
 switch( (*p) ) {
  case 69: goto st42;
  case 95: goto tr43;
  case 101: goto st42;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st42:
 if ( ++p == pe )
  goto _test_eof42;
case 42:
 switch( (*p) ) {
  case 69: goto st43;
  case 95: goto tr43;
  case 101: goto st43;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st43:
 if ( ++p == pe )
  goto _test_eof43;
case 43:
 switch( (*p) ) {
  case 78: goto tr83;
  case 95: goto tr43;
  case 110: goto tr83;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st44:
 if ( ++p == pe )
  goto _test_eof44;
case 44:
 switch( (*p) ) {
  case 69: goto st45;
  case 95: goto tr43;
  case 101: goto st45;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st45:
 if ( ++p == pe )
  goto _test_eof45;
case 45:
 switch( (*p) ) {
  case 83: goto st46;
  case 95: goto tr43;
  case 115: goto st46;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st46:
 if ( ++p == pe )
  goto _test_eof46;
case 46:
 switch( (*p) ) {
  case 67: goto tr86;
  case 95: goto tr43;
  case 99: goto tr86;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st47:
 if ( ++p == pe )
  goto _test_eof47;
case 47:
 switch( (*p) ) {
  case 65: goto st48;
  case 82: goto st51;
  case 95: goto tr43;
  case 97: goto st48;
  case 114: goto st51;
 }
 if ( (*p) < 66 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 98 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st48:
 if ( ++p == pe )
  goto _test_eof48;
case 48:
 switch( (*p) ) {
  case 76: goto st49;
  case 95: goto tr43;
  case 108: goto st49;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st49:
 if ( ++p == pe )
  goto _test_eof49;
case 49:
 switch( (*p) ) {
  case 83: goto st50;
  case 95: goto tr43;
  case 115: goto st50;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st50:
 if ( ++p == pe )
  goto _test_eof50;
case 50:
 switch( (*p) ) {
  case 69: goto tr91;
  case 95: goto tr43;
  case 101: goto tr91;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st51:
 if ( ++p == pe )
  goto _test_eof51;
case 51:
 switch( (*p) ) {
  case 79: goto st52;
  case 95: goto tr43;
  case 111: goto st52;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st52:
 if ( ++p == pe )
  goto _test_eof52;
case 52:
 switch( (*p) ) {
  case 77: goto tr93;
  case 95: goto tr43;
  case 109: goto tr93;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st53:
 if ( ++p == pe )
  goto _test_eof53;
case 53:
 switch( (*p) ) {
  case 82: goto st54;
  case 95: goto tr43;
  case 114: goto st54;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st54:
 if ( ++p == pe )
  goto _test_eof54;
case 54:
 switch( (*p) ) {
  case 79: goto st55;
  case 95: goto tr43;
  case 111: goto st55;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st55:
 if ( ++p == pe )
  goto _test_eof55;
case 55:
 switch( (*p) ) {
  case 85: goto st56;
  case 95: goto tr43;
  case 117: goto st56;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st56:
 if ( ++p == pe )
  goto _test_eof56;
case 56:
 switch( (*p) ) {
  case 80: goto tr97;
  case 95: goto tr43;
  case 112: goto tr97;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
tr97:
 {te = p+1;}
 goto st57;
st57:
 if ( ++p == pe )
  goto _test_eof57;
case 57:
 switch( (*p) ) {
  case 32: goto st15;
  case 95: goto tr43;
 }
 if ( (*p) < 48 ) {
  if ( 9 <= (*p) && (*p) <= 13 )
   goto st15;
 } else if ( (*p) > 57 ) {
  if ( (*p) > 90 ) {
   if ( 97 <= (*p) && (*p) <= 122 )
    goto tr43;
  } else if ( (*p) >= 65 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st15:
 if ( ++p == pe )
  goto _test_eof15;
case 15:
 switch( (*p) ) {
  case 32: goto st15;
  case 66: goto st16;
  case 98: goto st16;
 }
 if ( 9 <= (*p) && (*p) <= 13 )
  goto st15;
 goto tr18;
st16:
 if ( ++p == pe )
  goto _test_eof16;
case 16:
 switch( (*p) ) {
  case 89: goto tr21;
  case 121: goto tr21;
 }
 goto tr18;
st58:
 if ( ++p == pe )
  goto _test_eof58;
case 58:
 switch( (*p) ) {
  case 65: goto st59;
  case 95: goto tr43;
  case 97: goto st59;
 }
 if ( (*p) < 66 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 98 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st59:
 if ( ++p == pe )
  goto _test_eof59;
case 59:
 switch( (*p) ) {
  case 86: goto st60;
  case 95: goto tr43;
  case 118: goto st60;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st60:
 if ( ++p == pe )
  goto _test_eof60;
case 60:
 switch( (*p) ) {
  case 73: goto st61;
  case 95: goto tr43;
  case 105: goto st61;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st61:
 if ( ++p == pe )
  goto _test_eof61;
case 61:
 switch( (*p) ) {
  case 78: goto st62;
  case 95: goto tr43;
  case 110: goto st62;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st62:
 if ( ++p == pe )
  goto _test_eof62;
case 62:
 switch( (*p) ) {
  case 71: goto tr102;
  case 95: goto tr43;
  case 103: goto tr102;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st63:
 if ( ++p == pe )
  goto _test_eof63;
case 63:
 switch( (*p) ) {
  case 78: goto tr103;
  case 95: goto tr43;
  case 110: goto tr103;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st64:
 if ( ++p == pe )
  goto _test_eof64;
case 64:
 switch( (*p) ) {
  case 79: goto st65;
  case 95: goto tr43;
  case 111: goto st65;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st65:
 if ( ++p == pe )
  goto _test_eof65;
case 65:
 switch( (*p) ) {
  case 73: goto st66;
  case 95: goto tr43;
  case 105: goto st66;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st66:
 if ( ++p == pe )
  goto _test_eof66;
case 66:
 switch( (*p) ) {
  case 78: goto tr106;
  case 95: goto tr43;
  case 110: goto tr106;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st67:
 if ( ++p == pe )
  goto _test_eof67;
case 67:
 switch( (*p) ) {
  case 69: goto st68;
  case 73: goto st70;
  case 95: goto tr43;
  case 101: goto st68;
  case 105: goto st70;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st68:
 if ( ++p == pe )
  goto _test_eof68;
case 68:
 switch( (*p) ) {
  case 70: goto st69;
  case 95: goto tr43;
  case 102: goto st69;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st69:
 if ( ++p == pe )
  goto _test_eof69;
case 69:
 switch( (*p) ) {
  case 84: goto tr110;
  case 95: goto tr43;
  case 116: goto tr110;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st70:
 if ( ++p == pe )
  goto _test_eof70;
case 70:
 switch( (*p) ) {
  case 77: goto st71;
  case 95: goto tr43;
  case 109: goto st71;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st71:
 if ( ++p == pe )
  goto _test_eof71;
case 71:
 switch( (*p) ) {
  case 73: goto st72;
  case 95: goto tr43;
  case 105: goto st72;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st72:
 if ( ++p == pe )
  goto _test_eof72;
case 72:
 switch( (*p) ) {
  case 84: goto tr113;
  case 95: goto tr43;
  case 116: goto tr113;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st73:
 if ( ++p == pe )
  goto _test_eof73;
case 73:
 switch( (*p) ) {
  case 79: goto st74;
  case 85: goto st75;
  case 95: goto tr43;
  case 111: goto st74;
  case 117: goto st75;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st74:
 if ( ++p == pe )
  goto _test_eof74;
case 74:
 switch( (*p) ) {
  case 84: goto tr116;
  case 95: goto tr43;
  case 116: goto tr116;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st75:
 if ( ++p == pe )
  goto _test_eof75;
case 75:
 switch( (*p) ) {
  case 76: goto st76;
  case 95: goto tr43;
  case 108: goto st76;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st76:
 if ( ++p == pe )
  goto _test_eof76;
case 76:
 switch( (*p) ) {
  case 76: goto tr118;
  case 95: goto tr43;
  case 108: goto tr118;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st77:
 if ( ++p == pe )
  goto _test_eof77;
case 77:
 switch( (*p) ) {
  case 78: goto tr119;
  case 82: goto st78;
  case 95: goto tr43;
  case 110: goto tr119;
  case 114: goto st78;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st78:
 if ( ++p == pe )
  goto _test_eof78;
case 78:
 switch( (*p) ) {
  case 68: goto st79;
  case 95: goto tr43;
  case 100: goto st79;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr121;
st79:
 if ( ++p == pe )
  goto _test_eof79;
case 79:
 switch( (*p) ) {
  case 69: goto st80;
  case 95: goto tr43;
  case 101: goto st80;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st80:
 if ( ++p == pe )
  goto _test_eof80;
case 80:
 switch( (*p) ) {
  case 82: goto tr124;
  case 95: goto tr43;
  case 114: goto tr124;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
tr124:
 {te = p+1;}
 goto st81;
st81:
 if ( ++p == pe )
  goto _test_eof81;
case 81:
 switch( (*p) ) {
  case 32: goto st17;
  case 95: goto tr43;
 }
 if ( (*p) < 48 ) {
  if ( 9 <= (*p) && (*p) <= 13 )
   goto st17;
 } else if ( (*p) > 57 ) {
  if ( (*p) > 90 ) {
   if ( 97 <= (*p) && (*p) <= 122 )
    goto tr43;
  } else if ( (*p) >= 65 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st17:
 if ( ++p == pe )
  goto _test_eof17;
case 17:
 switch( (*p) ) {
  case 32: goto st17;
  case 66: goto st18;
  case 98: goto st18;
 }
 if ( 9 <= (*p) && (*p) <= 13 )
  goto st17;
 goto tr18;
st18:
 if ( ++p == pe )
  goto _test_eof18;
case 18:
 switch( (*p) ) {
  case 89: goto tr24;
  case 121: goto tr24;
 }
 goto tr18;
st82:
 if ( ++p == pe )
  goto _test_eof82;
case 82:
 switch( (*p) ) {
  case 82: goto st83;
  case 95: goto tr43;
  case 114: goto st83;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st83:
 if ( ++p == pe )
  goto _test_eof83;
case 83:
 switch( (*p) ) {
  case 65: goto st84;
  case 85: goto st90;
  case 95: goto tr43;
  case 97: goto st84;
  case 117: goto st90;
 }
 if ( (*p) < 66 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 98 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st84:
 if ( ++p == pe )
  goto _test_eof84;
case 84:
 switch( (*p) ) {
  case 78: goto st85;
  case 95: goto tr43;
  case 110: goto st85;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st85:
 if ( ++p == pe )
  goto _test_eof85;
case 85:
 switch( (*p) ) {
  case 83: goto st86;
  case 95: goto tr43;
  case 115: goto st86;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st86:
 if ( ++p == pe )
  goto _test_eof86;
case 86:
 switch( (*p) ) {
  case 70: goto st87;
  case 95: goto tr43;
  case 102: goto st87;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st87:
 if ( ++p == pe )
  goto _test_eof87;
case 87:
 switch( (*p) ) {
  case 79: goto st88;
  case 95: goto tr43;
  case 111: goto st88;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st88:
 if ( ++p == pe )
  goto _test_eof88;
case 88:
 switch( (*p) ) {
  case 82: goto st89;
  case 95: goto tr43;
  case 114: goto st89;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st89:
 if ( ++p == pe )
  goto _test_eof89;
case 89:
 switch( (*p) ) {
  case 77: goto tr133;
  case 95: goto tr43;
  case 109: goto tr133;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st90:
 if ( ++p == pe )
  goto _test_eof90;
case 90:
 switch( (*p) ) {
  case 69: goto tr134;
  case 95: goto tr43;
  case 101: goto tr134;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st91:
 if ( ++p == pe )
  goto _test_eof91;
case 91:
 switch( (*p) ) {
  case 83: goto st92;
  case 95: goto tr43;
  case 115: goto st92;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st92:
 if ( ++p == pe )
  goto _test_eof92;
case 92:
 switch( (*p) ) {
  case 73: goto st93;
  case 95: goto tr43;
  case 105: goto st93;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st93:
 if ( ++p == pe )
  goto _test_eof93;
case 93:
 switch( (*p) ) {
  case 78: goto st94;
  case 95: goto tr43;
  case 110: goto st94;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st94:
 if ( ++p == pe )
  goto _test_eof94;
case 94:
 switch( (*p) ) {
  case 71: goto tr138;
  case 95: goto tr43;
  case 103: goto tr138;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st95:
 if ( ++p == pe )
  goto _test_eof95;
case 95:
 switch( (*p) ) {
  case 72: goto st96;
  case 73: goto st99;
  case 95: goto tr43;
  case 104: goto st96;
  case 105: goto st99;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st96:
 if ( ++p == pe )
  goto _test_eof96;
case 96:
 switch( (*p) ) {
  case 69: goto st97;
  case 95: goto tr43;
  case 101: goto st97;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st97:
 if ( ++p == pe )
  goto _test_eof97;
case 97:
 switch( (*p) ) {
  case 82: goto st98;
  case 95: goto tr43;
  case 114: goto st98;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st98:
 if ( ++p == pe )
  goto _test_eof98;
case 98:
 switch( (*p) ) {
  case 69: goto tr143;
  case 95: goto tr43;
  case 101: goto tr143;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st99:
 if ( ++p == pe )
  goto _test_eof99;
case 99:
 switch( (*p) ) {
  case 84: goto st100;
  case 95: goto tr43;
  case 116: goto st100;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st100:
 if ( ++p == pe )
  goto _test_eof100;
case 100:
 switch( (*p) ) {
  case 72: goto tr145;
  case 95: goto tr43;
  case 104: goto tr145;
 }
 if ( (*p) < 65 ) {
  if ( 48 <= (*p) && (*p) <= 57 )
   goto tr43;
 } else if ( (*p) > 90 ) {
  if ( 97 <= (*p) && (*p) <= 122 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
tr145:
 {te = p+1;}
 goto st101;
st101:
 if ( ++p == pe )
  goto _test_eof101;
case 101:
 switch( (*p) ) {
  case 32: goto st19;
  case 95: goto tr43;
 }
 if ( (*p) < 48 ) {
  if ( 9 <= (*p) && (*p) <= 13 )
   goto st19;
 } else if ( (*p) > 57 ) {
  if ( (*p) > 90 ) {
   if ( 97 <= (*p) && (*p) <= 122 )
    goto tr43;
  } else if ( (*p) >= 65 )
   goto tr43;
 } else
  goto tr43;
 goto tr71;
st19:
 if ( ++p == pe )
  goto _test_eof19;
case 19:
 switch( (*p) ) {
  case 32: goto st19;
  case 84: goto st20;
  case 116: goto st20;
 }
 if ( 9 <= (*p) && (*p) <= 13 )
  goto st19;
 goto tr18;
st20:
 if ( ++p == pe )
  goto _test_eof20;
case 20:
 switch( (*p) ) {
  case 79: goto st21;
  case 111: goto st21;
 }
 goto tr18;
st21:
 if ( ++p == pe )
  goto _test_eof21;
case 21:
 switch( (*p) ) {
  case 84: goto st22;
  case 116: goto st22;
 }
 goto tr18;
st22:
 if ( ++p == pe )
  goto _test_eof22;
case 22:
 switch( (*p) ) {
  case 65: goto st23;
  case 97: goto st23;
 }
 goto tr18;
st23:
 if ( ++p == pe )
  goto _test_eof23;
case 23:
 switch( (*p) ) {
  case 76: goto st24;
  case 108: goto st24;
 }
 goto tr18;
st24:
 if ( ++p == pe )
  goto _test_eof24;
case 24:
 switch( (*p) ) {
  case 83: goto tr31;
  case 115: goto tr31;
 }
 goto tr18;
tr146:
 {te = p+1;}
 goto st102;
tr147:
 {te = p+1;{
            if (++rd == 1) {
                rs = p + 1;
            }
        }}
 goto st102;
tr148:
 cs = 102;
 {te = p+1;{
            if (--rd == 0) {
                re = p;
                type = TToken::Identifier;
                value->build(TStringBuf(rs, re));
                cs = 25;
                {p++; goto _out;}
            }
        }}
 goto _again;
st102:
 {ts = 0;}
 if ( ++p == pe )
  goto _test_eof102;
case 102:
 {ts = p;}
 switch( (*p) ) {
  case 0: goto st0;
  case 91: goto tr147;
  case 93: goto tr148;
 }
 goto tr146;
 }
 _test_eof25: cs = 25; goto _test_eof; 
 _test_eof26: cs = 26; goto _test_eof; 
 _test_eof1: cs = 1; goto _test_eof; 
 _test_eof2: cs = 2; goto _test_eof; 
 _test_eof3: cs = 3; goto _test_eof; 
 _test_eof27: cs = 27; goto _test_eof; 
 _test_eof4: cs = 4; goto _test_eof; 
 _test_eof5: cs = 5; goto _test_eof; 
 _test_eof6: cs = 6; goto _test_eof; 
 _test_eof7: cs = 7; goto _test_eof; 
 _test_eof8: cs = 8; goto _test_eof; 
 _test_eof9: cs = 9; goto _test_eof; 
 _test_eof10: cs = 10; goto _test_eof; 
 _test_eof11: cs = 11; goto _test_eof; 
 _test_eof12: cs = 12; goto _test_eof; 
 _test_eof28: cs = 28; goto _test_eof; 
 _test_eof29: cs = 29; goto _test_eof; 
 _test_eof13: cs = 13; goto _test_eof; 
 _test_eof14: cs = 14; goto _test_eof; 
 _test_eof30: cs = 30; goto _test_eof; 
 _test_eof31: cs = 31; goto _test_eof; 
 _test_eof32: cs = 32; goto _test_eof; 
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
 _test_eof44: cs = 44; goto _test_eof; 
 _test_eof45: cs = 45; goto _test_eof; 
 _test_eof46: cs = 46; goto _test_eof; 
 _test_eof47: cs = 47; goto _test_eof; 
 _test_eof48: cs = 48; goto _test_eof; 
 _test_eof49: cs = 49; goto _test_eof; 
 _test_eof50: cs = 50; goto _test_eof; 
 _test_eof51: cs = 51; goto _test_eof; 
 _test_eof52: cs = 52; goto _test_eof; 
 _test_eof53: cs = 53; goto _test_eof; 
 _test_eof54: cs = 54; goto _test_eof; 
 _test_eof55: cs = 55; goto _test_eof; 
 _test_eof56: cs = 56; goto _test_eof; 
 _test_eof57: cs = 57; goto _test_eof; 
 _test_eof15: cs = 15; goto _test_eof; 
 _test_eof16: cs = 16; goto _test_eof; 
 _test_eof58: cs = 58; goto _test_eof; 
 _test_eof59: cs = 59; goto _test_eof; 
 _test_eof60: cs = 60; goto _test_eof; 
 _test_eof61: cs = 61; goto _test_eof; 
 _test_eof62: cs = 62; goto _test_eof; 
 _test_eof63: cs = 63; goto _test_eof; 
 _test_eof64: cs = 64; goto _test_eof; 
 _test_eof65: cs = 65; goto _test_eof; 
 _test_eof66: cs = 66; goto _test_eof; 
 _test_eof67: cs = 67; goto _test_eof; 
 _test_eof68: cs = 68; goto _test_eof; 
 _test_eof69: cs = 69; goto _test_eof; 
 _test_eof70: cs = 70; goto _test_eof; 
 _test_eof71: cs = 71; goto _test_eof; 
 _test_eof72: cs = 72; goto _test_eof; 
 _test_eof73: cs = 73; goto _test_eof; 
 _test_eof74: cs = 74; goto _test_eof; 
 _test_eof75: cs = 75; goto _test_eof; 
 _test_eof76: cs = 76; goto _test_eof; 
 _test_eof77: cs = 77; goto _test_eof; 
 _test_eof78: cs = 78; goto _test_eof; 
 _test_eof79: cs = 79; goto _test_eof; 
 _test_eof80: cs = 80; goto _test_eof; 
 _test_eof81: cs = 81; goto _test_eof; 
 _test_eof17: cs = 17; goto _test_eof; 
 _test_eof18: cs = 18; goto _test_eof; 
 _test_eof82: cs = 82; goto _test_eof; 
 _test_eof83: cs = 83; goto _test_eof; 
 _test_eof84: cs = 84; goto _test_eof; 
 _test_eof85: cs = 85; goto _test_eof; 
 _test_eof86: cs = 86; goto _test_eof; 
 _test_eof87: cs = 87; goto _test_eof; 
 _test_eof88: cs = 88; goto _test_eof; 
 _test_eof89: cs = 89; goto _test_eof; 
 _test_eof90: cs = 90; goto _test_eof; 
 _test_eof91: cs = 91; goto _test_eof; 
 _test_eof92: cs = 92; goto _test_eof; 
 _test_eof93: cs = 93; goto _test_eof; 
 _test_eof94: cs = 94; goto _test_eof; 
 _test_eof95: cs = 95; goto _test_eof; 
 _test_eof96: cs = 96; goto _test_eof; 
 _test_eof97: cs = 97; goto _test_eof; 
 _test_eof98: cs = 98; goto _test_eof; 
 _test_eof99: cs = 99; goto _test_eof; 
 _test_eof100: cs = 100; goto _test_eof; 
 _test_eof101: cs = 101; goto _test_eof; 
 _test_eof19: cs = 19; goto _test_eof; 
 _test_eof20: cs = 20; goto _test_eof; 
 _test_eof21: cs = 21; goto _test_eof; 
 _test_eof22: cs = 22; goto _test_eof; 
 _test_eof23: cs = 23; goto _test_eof; 
 _test_eof24: cs = 24; goto _test_eof; 
 _test_eof102: cs = 102; goto _test_eof; 

 _test_eof: {}
 if ( p == eof )
 {
 switch ( cs ) {
 case 26: goto tr58;
 case 27: goto tr59;
 case 4: goto tr5;
 case 5: goto tr5;
 case 6: goto tr5;
 case 7: goto tr5;
 case 8: goto tr5;
 case 9: goto tr5;
 case 10: goto tr5;
 case 28: goto tr59;
 case 29: goto tr63;
 case 13: goto tr15;
 case 14: goto tr15;
 case 30: goto tr63;
 case 31: goto tr65;
 case 32: goto tr59;
 case 33: goto tr59;
 case 34: goto tr71;
 case 35: goto tr74;
 case 36: goto tr71;
 case 37: goto tr76;
 case 38: goto tr71;
 case 39: goto tr71;
 case 40: goto tr71;
 case 41: goto tr71;
 case 42: goto tr71;
 case 43: goto tr71;
 case 44: goto tr71;
 case 45: goto tr71;
 case 46: goto tr71;
 case 47: goto tr71;
 case 48: goto tr71;
 case 49: goto tr71;
 case 50: goto tr71;
 case 51: goto tr71;
 case 52: goto tr71;
 case 53: goto tr71;
 case 54: goto tr71;
 case 55: goto tr71;
 case 56: goto tr71;
 case 57: goto tr71;
 case 15: goto tr18;
 case 16: goto tr18;
 case 58: goto tr71;
 case 59: goto tr71;
 case 60: goto tr71;
 case 61: goto tr71;
 case 62: goto tr71;
 case 63: goto tr71;
 case 64: goto tr71;
 case 65: goto tr71;
 case 66: goto tr71;
 case 67: goto tr71;
 case 68: goto tr71;
 case 69: goto tr71;
 case 70: goto tr71;
 case 71: goto tr71;
 case 72: goto tr71;
 case 73: goto tr71;
 case 74: goto tr71;
 case 75: goto tr71;
 case 76: goto tr71;
 case 77: goto tr71;
 case 78: goto tr121;
 case 79: goto tr71;
 case 80: goto tr71;
 case 81: goto tr71;
 case 17: goto tr18;
 case 18: goto tr18;
 case 82: goto tr71;
 case 83: goto tr71;
 case 84: goto tr71;
 case 85: goto tr71;
 case 86: goto tr71;
 case 87: goto tr71;
 case 88: goto tr71;
 case 89: goto tr71;
 case 90: goto tr71;
 case 91: goto tr71;
 case 92: goto tr71;
 case 93: goto tr71;
 case 94: goto tr71;
 case 95: goto tr71;
 case 96: goto tr71;
 case 97: goto tr71;
 case 98: goto tr71;
 case 99: goto tr71;
 case 100: goto tr71;
 case 101: goto tr71;
 case 19: goto tr18;
 case 20: goto tr18;
 case 21: goto tr18;
 case 22: goto tr18;
 case 23: goto tr18;
 case 24: goto tr18;
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

} // namespace NAst
} // namespace NQueryClient
} // namespace NYT

