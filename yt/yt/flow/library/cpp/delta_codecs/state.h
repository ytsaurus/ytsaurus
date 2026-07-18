#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NFlow::NDeltaCodecs {

////////////////////////////////////////////////////////////////////////////////

struct TState
{
    TSharedRef Base;
    TSharedRef Patch;
};

struct TStateMutation
{
    std::optional<TSharedRef> Base;
    std::optional<TSharedRef> Patch;
};

TStateMutation MutateState(const ICodec* codec, const TState& state, const TSharedRef& value, EAlgorithm algorithm);

TState ApplyStateMutation(const TState& state, const TStateMutation& patch);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDeltaCodecs
