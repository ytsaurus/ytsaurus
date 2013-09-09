#include "stdafx.h"
#include "lexer.h"

#include "token.h"
#include "zigzag.h"
#include "varint.h"

#include "lexer_detail.h"

#include <core/misc/error.h>
#include <core/misc/property.h>

#include <util/string/escape.h>

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

class TStatelessLexer::TImpl
{
private:
    std::unique_ptr<TStatelessYsonLexerImplBase> Impl;

public:
    TImpl(bool enableLinePositionInfo = false)
        : Impl(enableLinePositionInfo
        ? static_cast<TStatelessYsonLexerImplBase*>(new TStatelesYsonLexerImpl<true>()) 
        : static_cast<TStatelessYsonLexerImplBase*>(new TStatelesYsonLexerImpl<false>()))
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
