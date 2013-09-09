#pragma once

#include <core/misc/common.h>

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

//! The data format.
DECLARE_ENUM(EYsonFormat,
    // Binary.
    // Most compact but not human-readable.
    (Binary)

    // Text.
    // Not so compact but human-readable.
    // Does not use indentation.
    // Uses escaping for non-text characters.
    (Text)

    // Text with indentation.
    // Extremely verbose but human-readable.
    // Uses escaping for non-text characters.
    (Pretty)
);

DECLARE_ENUM(EYsonType,
    (Node)
    (ListFragment)
    (MapFragment)
);

class ETokenType;

class TTokenizer;

struct IYsonConsumer;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
