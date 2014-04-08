#pragma once

#include "public.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TUtf8Decoder
{
public:
    explicit TUtf8Decoder(bool enableEscaping = true);

    TStringBuf Encode(const TStringBuf& str);
    TStringBuf Decode(const TStringBuf& str);
private:
    bool EnableEscaping_;
    std::vector<char> Buffer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
