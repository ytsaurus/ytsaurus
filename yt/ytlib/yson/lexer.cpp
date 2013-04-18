#include "stdafx.h"
#include "lexer.h"

#include "token.h"
#include "zigzag.h"
#include "varint.h"

#include <ytlib/misc/error.h>
#include <ytlib/misc/property.h>

#include <util/string/escape.h>

#include "yson_lexer_detail.h"

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////


class TStatelessLexer::TImpl
{
private:
    typedef NDetail::TLexer<TStringReader, false> TLexer;
    TLexer Lexer;

public:
    TImpl()
        : Lexer(TStringReader())
    { }
    
    size_t GetToken(const TStringBuf& data, TToken* token)
    {
        Lexer.SetBuffer(data.begin(), data.end());
        Lexer.GetToken(token);
        return Lexer.Begin() - data.begin();
    }
};

////////////////////////////////////////////////////////////////////////////////

TStatelessLexer::TStatelessLexer()
    : Impl(new TImpl())
{ }

TStatelessLexer::~TStatelessLexer()
{ }

size_t TStatelessLexer::GetToken(const TStringBuf& data, TToken* token)
{
    return Impl->GetToken(data, token);
}

} // namespace NYson
} // namespace NYT
