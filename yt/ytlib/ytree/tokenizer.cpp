#include "stdafx.h"
#include "tokenizer.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TTokenizer::TTokenizer(const TStringBuf& input)
    : Input(input)
{ }

const TToken& TTokenizer::operator[](size_t index)
{
    ChopTo(index);
    YASSERT(Tokens.size() > index);
    return Tokens[index];
}

TStringBuf TTokenizer::GetSuffix(size_t index)
{
    ChopTo(index);
    YASSERT(SuffixPositions.size() > index);
    return Input.SubStr(SuffixPositions[index]);
}

void TTokenizer::ChopTo(size_t index)
{
    if (Tokens.empty()) {
        ChopToken(0);
    }
    while (Tokens.size() <= index) {
        YASSERT(!Tokens.back().IsEmpty());
        ChopToken(SuffixPositions.back());
    }
}

void TTokenizer::ChopToken(size_t position)
{
    while (Lexer.GetState() != TLexer::EState::Terminal && position <  Input.length()) {
        if (Lexer.Consume(Input[position])) {
            ++position;
        }
    }
    Lexer.Finish();

    Tokens.push_back(
        Lexer.GetState() == TLexer::EState::Terminal
            ? Lexer.GetToken() // TODO(roizner): Fix this once TToken contains TStringBuf instead of Stroka
            : TToken::EndOfStream);
    SuffixPositions.push_back(position);
    YASSERT(Tokens.size() == SuffixPositions.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtree
} // namespace NYT
