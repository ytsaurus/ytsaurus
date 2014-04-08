#pragma once

#include <core/misc/common.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TUtf8Decoder
{
public:
    explicit TUtf8Decoder(bool enableEscaping = true);

    TStringBuf Encode(TStringBuf str);
    TStringBuf Decode(TStringBuf str);
private:
    bool EnableEscaping_;
    std::vector<char> Buffer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
