#pragma once

#include "source.h"

#include <ytlib/actions/callback.h>

namespace NYT {

typedef TCallback<void (StreamSource*, std::vector<char>*)> TConverter;

//TODO(ignat): rename this methods
inline TSharedRef Apply(TConverter converter, const TSharedRef& ref)
{
    ByteArraySource source(ref.Begin(), ref.Size());
    std::vector<char> output;
    converter.Run(&source, &output);
    return TSharedRef(MoveRV(output));
}

inline TSharedRef Apply(TConverter converter, const std::vector<TSharedRef>& refs)
{
    if (refs.size() == 1) {
        return Apply(converter, refs.front());
    }
    VectorRefsSource source(refs);
    std::vector<char> output;
    converter.Run(&source, &output);
    return TSharedRef(MoveRV(output));
}

} // namespace NYT
