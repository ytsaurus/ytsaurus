#pragma once

#ifndef LEXICOGRAPHICALLY_SERIALIZE_INL_H_
    #error "Direct inclusion of this file is not allowed, include lexicographically_serialize.h"
    // For the sake of sane code completion.
    #include "lexicographically_serialize.h"
#endif

#include <library/cpp/yt/error/error.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
T LexicographicallyParse(TStringBuf serialized)
{
    T destination = {};
    TStringBuf mutableSerialized = serialized;
    LexicographicallyRead(mutableSerialized, destination);
    THROW_ERROR_EXCEPTION_UNLESS(mutableSerialized.empty(),
        "Serialized value %Qv contains %v excess bytes",
        serialized,
        mutableSerialized.size());
    return destination;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
