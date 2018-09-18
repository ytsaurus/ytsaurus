#include "lexer.h"
#include "lexer_detail.h"

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

TStatelessLexer::TStatelessLexer()
    : Impl(std::make_unique<TStatelesYsonLexerImpl<false>>())
{ }

TStatelessLexer::~TStatelessLexer()
{ }

size_t TStatelessLexer::GetToken(TStringBuf data, TToken* token)
{
    return Impl->GetToken(data, token);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
