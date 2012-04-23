#pragma once

#include "public.h"
#include "lexer.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TTokenizer {
public:
    TTokenizer(const TStringBuf& input);

    const TToken& operator[](size_t index);
    TStringBuf GetSuffix(size_t index);

    /*
     * Consider implementing the following methods if needed:
     * int GetChoppedTokenCount() const;
     * bool Finished() const;
     */

private:
    void ChopTo(size_t index);
    void ChopToken(size_t position);

    TStringBuf Input;
    TLexer Lexer;
    std::vector<TToken> Tokens;
    std::vector<size_t> SuffixPositions;
    std::vector<Stroka> StringBuffers; // for String tokens
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYtree
} // namespace NYT
