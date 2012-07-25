#pragma once

#include "source.h"

#include <ytlib/actions/callback.h>

namespace NYT {

typedef TCallback<void (StreamSource*, std::vector<char>*)> Converter;

//TODO(ignat): rename this methods
inline TSharedRef Perform(const TSharedRef& ref, Converter convert)
{
    ByteArraySource source(ref.Begin(), ref.Size());
    std::vector<char> output;
    convert.Run(&source, &output);
    return TSharedRef(MoveRV(output));
}

inline TSharedRef Perform(const std::vector<TSharedRef>& refs, Converter convert)
{
    if (refs.size() == 1) {
        return Perform(refs.front(), convert);
    }
    VectorRefsSource source(refs);
    std::vector<char> output;
    convert.Run(&source, &output);
    return TSharedRef(MoveRV(output));
}

} // namespace NYT
