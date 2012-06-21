#pragma once

#include "public.h"
#include "lexer.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

// TODO(roizner): document
class TTokenizer
{
public:
    explicit TTokenizer(const TStringBuf& input);

    bool ParseNext();
    const TToken& CurrentToken() const;
    ETokenType GetCurrentType() const;
    TStringBuf GetCurrentSuffix() const;
    const TStringBuf& CurrentInput() const;

private:
    TStringBuf Input;
    TLexer Lexer;
    size_t Parsed;
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYtree
} // namespace NYT
