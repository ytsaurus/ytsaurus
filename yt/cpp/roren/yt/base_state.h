#pragma once

#include <yt/cpp/roren/interface/type_tag.h>
#include <yt/cpp/roren/interface/private/raw_pipeline.h>
#include <yt/cpp/roren/interface/private/save_loadable_pointer_wrapper.h>

#include <util/generic/fwd.h>
#include <util/ysaveload.h>

template <typename T>
concept IsOptional = std::same_as<T, std::optional<typename T::value_type>>;

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

struct TYtStateVtable
{
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

    Y_SAVELOAD_DEFINE(
        StateTKVvtable,
        SaveLoadablePointer(LoadState),
        SaveLoadablePointer(SaveState),
        SaveLoadablePointer(StateFromKey),
        SaveLoadablePointer(StateFromTKV)
    );
};

////////////////////////////////////////////////////////////////////////////////

extern TTypeTag<TYtStateVtable> YtStateVtableTag;
extern TTypeTag<TString> YtStateInPathTag;
extern TTypeTag<TString> YtStateOutPathTag;

void InitializeYtPState(TRawPStateNodePtr pState, TString inStatePath, TString outStatePath, TYtStateVtable stateVtable);

template <typename TKey, typename TState>
TPState<TKey, TState> MakeYtPState(const TPipeline& YtPipeline, TString inStatePath, TString outStatePath, TYtStateVtable stateVtable)
{
    auto rawPipeline = NPrivate::GetRawPipeline(YtPipeline);
    auto pState = NPrivate::MakePState<TKey, TState>(rawPipeline);
    auto rawPState = NPrivate::GetRawPStateNode(pState);

    InitializeYtPState(rawPState, std::move(inStatePath), std::move(outStatePath), std::move(stateVtable));
    return pState;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
