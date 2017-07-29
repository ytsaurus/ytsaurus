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

TStatelessLexer::TStatelessLexer()
    : Impl(std::make_unique<TStatelesYsonLexerImpl<false>>())
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
