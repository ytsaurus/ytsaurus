#pragma once

#include "public.h"
#include "token.h"

#include <core/misc/nullable.h>

#include <util/memory/tempbuf.h>

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
    const Stroka& GetLiteralValue() const;

    void Expect(ETokenType expectedType);
    void ThrowUnexpected();

private:
    TYPath Path_;

    ETokenType Type_;
    TStringBuf Token_;
    TStringBuf Input_;
    Stroka LiteralValue_;

    const char* AdvanceEscaped(const char* current);
    static int ParseHexDigit(char ch, const TStringBuf& context);
    static void ThrowMalformedEscapeSequence(const TStringBuf& context);

};

////////////////////////////////////////////////////////////////////////////////

bool HasPrefix(const TYPath& fullPath, const TYPath& prefixPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYPath
} // namespace NYT
