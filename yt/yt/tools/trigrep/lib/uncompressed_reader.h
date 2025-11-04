#pragma once

#include "reader.h"

#include <string>

namespace NYT::NTrigrep {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ISequentialReader> CreateSequentialUncompressedReader(
    const std::string& path,
    i64 maxFrameSize);

std::unique_ptr<IRandomReader> CreateRandomUncompressedReader(
    const std::string& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep
