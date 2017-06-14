#pragma once

#include "token.h"

namespace NYT {
namespace NYPath {

////////////////////////////////////////////////////////////////////////////////

class TTokenizer
{
public:
    explicit TTokenizer(const TYPath& path);

    ETokenType Advance();

    ETokenType GetType() const;
    const TStringBuf& GetToken() const;
    TYPath GetPrefix() const;
    TYPath GetSuffix() const;
    TYPath GetInput() const;
    const TString& GetLiteralValue() const;

    void Expect(ETokenType expectedType);
    void Skip(ETokenType expectedType);
    void ThrowUnexpected();

private:
    const TYPath Path_;

    ETokenType Type_;
    ETokenType PreviousType_;
    TStringBuf Token_;
    TStringBuf Input_;
    TString LiteralValue_;

    void SetType(ETokenType type);
    const char* AdvanceEscaped(const char* current);
    static int ParseHexDigit(char ch, const TStringBuf& context);
    static void ThrowMalformedEscapeSequence(const TStringBuf& context);

};

////////////////////////////////////////////////////////////////////////////////

bool HasPrefix(const TYPath& fullPath, const TYPath& prefixPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYPath
} // namespace NYT
