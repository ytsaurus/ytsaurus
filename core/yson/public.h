#pragma once

#include <yt/core/misc/public.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! The data format.
DEFINE_ENUM(EYsonFormat,
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

// NB: -1 is used for serializing null TYsonString.
DEFINE_ENUM(EYsonType,
    ((Node)          (0))
    ((ListFragment)  (1))
    ((MapFragment)   (2))
);

enum class ETokenType;

class TYsonString;

class TYsonProducer;

class TYsonInput;
class TYsonOutput;

class TTokenizer;

class TProtobufMessageType;

struct IYsonConsumer;
struct IFlushableYsonConsumer;
struct IAsyncYsonConsumer;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
