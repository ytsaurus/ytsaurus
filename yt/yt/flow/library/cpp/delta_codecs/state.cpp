#include "state.h"

#include "codec.h"

namespace NYT::NFlow::NDeltaCodecs {

////////////////////////////////////////////////////////////////////////////////

TStateMutation MutateState(const ICodec* codec, const TState& state, const TSharedRef& value, EAlgorithm algorithm)
{
    auto newState = state;

    if (state.Base.ToStringBuf().empty() || value.ToStringBuf().empty() || algorithm == EAlgorithm::ZeroPatch) {
        newState = TState{.Base = value, .Patch = TSharedRef::MakeEmpty()};
    } else if (auto patch = codec->TryComputePatch(state.Base, value)) {
        if (algorithm == EAlgorithm::ForcePatch) {
            newState = TState{.Base = state.Base, .Patch = std::move(*patch)};
        } else if (algorithm == EAlgorithm::SizeHeuristics) {
            if (RandomNumber(value.ToStringBuf().size()) < patch->ToStringBuf().size()) {
                newState = TState{.Base = value, .Patch = TSharedRef::MakeEmpty()};
            } else {
                newState = TState{.Base = state.Base, .Patch = std::move(*patch)};
            }
        } else {
            YT_ABORT("Unreachable");
        }
    } else {
        newState = TState{.Base = value, .Patch = TSharedRef::MakeEmpty()};
    }

    return {
        newState.Base.ToStringBuf() != state.Base.ToStringBuf() ? std::optional(newState.Base) : std::nullopt,
        newState.Patch.ToStringBuf() != state.Patch.ToStringBuf() ? std::optional(newState.Patch) : std::nullopt,
    };
}

TState ApplyStateMutation(const TState& state, const TStateMutation& mutation)
{
    return {
        mutation.Base ? *mutation.Base : state.Base,
        mutation.Patch ? *mutation.Patch : state.Patch,
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDeltaCodecs
