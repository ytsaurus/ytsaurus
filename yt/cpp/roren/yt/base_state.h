#pragma once

#include <util/generic/fwd.h>
#include <util/ysaveload.h>
#include <concepts>
#include <optional>

#include <yt/cpp/roren/interface/type_tag.h>
#include <yt/cpp/roren/interface/private/raw_pipeline.h>

template <typename T>
concept is_optional = std::same_as<T, std::optional<typename T::value_type>>;

namespace NRoren::NPrivate
{

struct TYtStateVtable
{
    template <class TState>
    using TStateTKV = TKV<typename TState::TKey, typename TState::TValue>;
    using TLoadState = void (*)(TRawRowHolder& row, const NYT::TNode&);  // NYT::TNode->TStateTKV
    using TSaveState = void (*)(::NYson::TYsonWriter& writer, void* rawState, const void* rawTKV); // TState -> Cout
    using TStateFromKey = TRawRowHolder (*)(const void* rawKey); // TState::TKey -> TState
    using TStateFromTKV = TRawRowHolder (*)(const void* rawTKV); // TVK<TState::TKey, TState::TValue> -> TState

    TRowVtable StateTKVvtable;
    TLoadState LoadState = nullptr;
    TSaveState SaveState = nullptr;
    TStateFromKey StateFromKey = nullptr;
    TStateFromTKV StateFromTKV = nullptr;

    static NYT::TNode SerializableToNode(const TYtStateVtable& stateVtable);
    static TYtStateVtable SerializableFromNode(const NYT::TNode& node);
}; // struct YtStateVtable

////////////////////////////////////////////////////////////////////////////////

extern TTypeTag<TYtStateVtable> YtStateVtableTag;
extern TTypeTag<TString> YtStateInPathTag;
extern TTypeTag<TString> YtStateOutPathTag;


template <typename TKey, typename TState>
TPState<TKey, TState> MakeYtPState(const TPipeline& YtPipeline, TString in_state_path, TString out_state_path, TYtStateVtable stateVtable)
{
    auto rawPipeline = NPrivate::GetRawPipeline(YtPipeline);
    auto pState = NPrivate::MakePState<TKey, TState>(rawPipeline);
    auto rawPState = NPrivate::GetRawPStateNode(pState);

    InitializeYtPState(rawPState, std::move(in_state_path), std::move(out_state_path), std::move(stateVtable));
    return pState;
}

void InitializeYtPState(TRawPStateNodePtr pState, TString in_state_path, TString out_state_path, TYtStateVtable stateVtable);

} // namespace NRoren::NPrivate

template <>
class TSerializer<NRoren::NPrivate::TYtStateVtable>
{
public:
    using TYtStateVtable = NRoren::NPrivate::TYtStateVtable;

public:
    static void Save(IOutputStream* output, const TYtStateVtable& stateVtable);
    static void Load(IInputStream* input, TYtStateVtable& stateVtable);
};

