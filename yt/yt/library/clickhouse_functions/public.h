#pragma once

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

//! YSON formats supported in CHYT for better user experience (CHYT-514).
DEFINE_ENUM(EExtendedYsonFormat,
    // Regular formats from EYsonFormat.
    (Binary)
    (Text)
    (Pretty)
    // Unescaped* analogs. They are similar to regular ones, but they do not escape string literals.
    // These are used to display unicode strings in a human-readable format.
    // Only the control characters are escaped (e.g. '\\', '\"', '\r' and '\n').
    (UnescapedText)
    (UnescapedPretty)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
