#pragma once

#include "public.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TUtf8Transcoder
{
public:
    explicit TUtf8Transcoder(bool enableEncoding = true);

    TStringBuf Encode(const TStringBuf& str);
    TStringBuf Decode(const TStringBuf& str);

private:
    bool EnableEncoding_;
    std::vector<char> Buffer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
