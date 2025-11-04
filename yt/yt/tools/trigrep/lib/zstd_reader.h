#pragma once

#include "reader.h"

#include <string>

namespace NYT::NTrigrep {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ISequentialReader> CreateSequentialZstdReader(
    const std::string& path);

std::unique_ptr<IRandomReader> CreateRandomZstdReader(
    const std::string& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep
