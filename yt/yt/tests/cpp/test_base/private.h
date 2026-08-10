#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NCppTests {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, CppTestsLogger, "CppTests");

// std::optional<

// const NLogging::TLogger& CppTestsLogger()
// {
//     static char uni
// }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCppTests
