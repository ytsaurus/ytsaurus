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
    THolder<TYsonStatelessLexerImplBase> Impl;

public:
    TImpl(bool enableLinePositionInfo = false)
        : Impl(enableLinePositionInfo? 
        static_cast<TYsonStatelessLexerImplBase*>(new TYsonStatelessLexerImpl<true>()) 
        : static_cast<TYsonStatelessLexerImplBase*>(new TYsonStatelessLexerImpl<false>()))
    { }

    size_t GetToken(const TStringBuf& data, TToken* token)
    {
        return Impl->GetToken(data, token);
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
