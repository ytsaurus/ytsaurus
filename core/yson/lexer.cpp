#include "lexer.h"
#include "lexer_detail.h"
#include "token.h"

#include <yt/core/misc/error.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/varint.h>
#include <yt/core/misc/zigzag.h>

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
