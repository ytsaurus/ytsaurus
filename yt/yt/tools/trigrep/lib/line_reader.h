#pragma once

#include "public.h"

#include <yt/yt/core/misc/public.h>

namespace NYT::NTrigrep {

////////////////////////////////////////////////////////////////////////////////

class TLineReader
{
public:
    explicit TLineReader(IInputStream* input);

    std::optional<TStringBuf> ReadLine();
    TChecksum GetChecksum() const;

private:
    IInputStream* const Input_;

    static constexpr i64 BufferCapacity = 4_KB;
    std::vector<char> Buffer_;
    i64 BufferPosition_ = 0;
    i64 BufferSize_ = 0;

    TChecksum Checksum_ = {};

    std::string ScratchLine_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep
