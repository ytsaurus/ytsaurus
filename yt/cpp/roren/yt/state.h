#pragma once

#include <yt/cpp/roren/interface/type_tag.h>
#include <yt/cpp/roren/interface/private/raw_pipeline.h>
#include <util/generic/fwd.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {
struct TYtStateVtable
{
    template <class TState>
    using TStateTKV = TKV<typename TState::TKey, typename TState::TValue>;
    using TLoadState = void (*)(TRawRowHolder& row, const NYT::TNode&);  // NYT::TNode->TStateTKV
    using TSaveState = void*;
    using TStateFromKey = TRawRowHolder (*)(const void* rawKey);
    using TStateFromTKV = TRawRowHolder (*)(void* rawTKV);

    TRowVtable StateTKVvtable;
    TLoadState LoadState = nullptr;
    TSaveState SaveState = nullptr;
    TStateFromKey StateFromKey = nullptr;
    TStateFromTKV StateFromTKV = nullptr;

    static NYT::TNode SerializableToNode(const TYtStateVtable& stateVtable);
    static TYtStateVtable SerializableFromNode(const NYT::TNode& node);
//TKV vtable
//TKV -> TState
//TState -> TNode
}; // struct YtStateVtable

extern TTypeTag<TYtStateVtable> YtStateVtableTag;
extern TTypeTag<TString> YtStateInPathTag;
extern TTypeTag<TString> YtStateOutPathTag;

Y_HAS_MEMBER(Base);

template <class T, std::enable_if_t<THasBase<T>::value, int> = 0>
inline TStringBuf Name(const T& column) {
    return column.Base();
}

template <class T, std::enable_if_t<!THasBase<T>::value, int> = 0>
inline TStringBuf Name(const T& column) {
    return column.Name();
}

template <class TColumnNames, class T>
inline void Unpack(T& dst, size_t i, const TColumnNames& columns, NYT::TNode node)
{
    if constexpr (std::is_same_v<std::optional<typename T::value_type>, T>) {
        dst = node[Name(columns[i])].template As<typename T::value_type>();
    } else {
        dst = node[Name(columns[i])].template As<T>();
    }
}

template <class TColumnNames, class TValuesPack>
inline void Unpack(TValuesPack& vp, const TColumnNames& columns, NYT::TNode node)
{
    auto f = [&] (auto& ... v) {
        size_t i = 0;
        (Unpack(v, i++, columns, node), ...);
    };
    std::apply(f, vp);
}


template <class TState>
void StateLoader(TRawRowHolder& row, const NYT::TNode& node)
{
    Unpack(*static_cast<typename TState::TKey*>(row.GetKeyOfKV()), TState::Schema().KeyColumns, node);
    Unpack(*static_cast<typename TState::TValue*>(row.GetValueOfKV()), TState::Schema().ValueColumns, node);
}


template <class TState>
TRawRowHolder StateFromKey(const void* rawKey)
{
    TRawRowHolder result(MakeRowVtable<TState>());
    auto* key = reinterpret_cast<const typename TState::TKey*>(rawKey);
    *reinterpret_cast<TState*>(result.GetData()) = TState(*key);
    return result;
}

template <class TState>
TRawRowHolder StateFromTVK(void* rawTKV)
{
    auto* tkv = reinterpret_cast<TYtStateVtable::TStateTKV<TState>*>(rawTKV);
    TRawRowHolder result(MakeRowVtable<TState>());
    TState& state = *reinterpret_cast<TState*>(result.GetData());
    state = TState(std::move(tkv->Key()), std::move(tkv->Value()));
    return result;
}

}  // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

template <typename TState>
TPState<typename TState::TKey, TState> MakeYtProfilePState(const TPipeline& YtPipeline, TString in_state_path, TString out_state_path = {})
{
    using NPrivate::TRowVtable;
    using NPrivate::TRawRowHolder;
    using NPrivate::MakeRowVtable;
    using NPrivate::TYtStateVtable;
    using NPrivate::StateLoader;
    using NPrivate::StateFromKey;
    using NPrivate::StateFromTVK;

    if (out_state_path.empty()) {
        out_state_path = in_state_path;
    }
    auto rawPipeline = NPrivate::GetRawPipeline(YtPipeline);
    auto pState = NPrivate::MakePState<typename TState::TKey, TState>(rawPipeline);
    auto rawPState = NPrivate::GetRawPStateNode(pState);

    NPrivate::TYtStateVtable stateVtable;
    stateVtable.StateTKVvtable = MakeRowVtable<TYtStateVtable::TStateTKV<TState>>();
    stateVtable.LoadState = &StateLoader<TState>;
    //stateVtable.SaveState =
    stateVtable.StateFromKey = &StateFromKey<TState>;
    stateVtable.StateFromTKV = &StateFromTVK<TState>;
    NPrivate::SetAttribute(*rawPState, NPrivate::YtStateVtableTag, std::move(stateVtable));
    NPrivate::SetAttribute(*rawPState, NPrivate::YtStateInPathTag, std::move(in_state_path));
    NPrivate::SetAttribute(*rawPState, NPrivate::YtStateOutPathTag, std::move(out_state_path));
    return pState;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren

template <>
class TSerializer<NRoren::NPrivate::TYtStateVtable>
{
public:
    using TYtStateVtable = NRoren::NPrivate::TYtStateVtable;

public:
    static void Save(IOutputStream* output, const TYtStateVtable& stateVtable);
    static void Load(IInputStream* input, TYtStateVtable& stateVtable);
};

