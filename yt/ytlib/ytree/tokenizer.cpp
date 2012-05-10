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
    return !CurrentToken().IsEmpty();
}

const TToken& TTokenizer::CurrentToken() const
{
    return Lexer.GetState() == TLexer::EState::Terminal
        ? Lexer.GetToken()
        : TToken::EndOfStream;
}

ETokenType TTokenizer::GetCurrentType() const
{
    return CurrentToken().GetType();
}

TStringBuf TTokenizer::GetCurrentSuffix() const
{
    return Input.Tail(Parsed);
}

const TStringBuf& TTokenizer::CurrentInput() const
{
    return Input;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtree
} // namespace NYT
