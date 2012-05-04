#include "stdafx.h"
#include "tokenizer.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TTokenizer::TTokenizer(const TStringBuf& input)
    : Input(input)
    , Parsed(0)
{ }

bool TTokenizer::ParseNext()
{
    Input = Input.Tail(Parsed);
    Lexer.Reset();
    Parsed = Lexer.Read(Input);
    Lexer.Finish();
    return !Current().IsEmpty();
}

const TToken& TTokenizer::Current() const
{
    return Lexer.GetState() == TLexer::EState::Terminal
        ? Lexer.GetToken()
        : TToken::EndOfStream;
}

ETokenType TTokenizer::GetCurrentType() const
{
    return Current().GetType();
}

TStringBuf TTokenizer::GetCurrentSuffix() const
{
    return Input.Tail(Parsed);
}

const TStringBuf& TTokenizer::GetCurrentInput() const
{
    return Input;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtree
} // namespace NYT
