#pragma once

#include "base_state.h"
#include <tuple>

namespace NRoren::NPrivate::NKvState {

////////////////////////////////////////////////////////////////////////////////

extern const TString KeyColumn;
extern const TString ValueColumn;

template <class T>
void LoadStateItem(T& dst, const TString& column, const NYT::TNode& node)
{
    if (!node.HasKey(column)) {
        return;
    }
    const auto& data = node[column];
    if (data.IsNull()) {
        return;
    }

    if constexpr (IsOptional<T>) {
        dst = data.template As<typename T::value_type>();
    } else {
        dst = data.template As<T>();
    }
}

template <class T>
void SaveStateItem(::NYson::TYsonWriter& writer, const T& item, const TString& column)
{
    if constexpr (IsOptional<T>) {
        if (item) {
            SaveStateItem(writer, item.value(), column);
        }
    } else {
        writer.OnKeyedItem(column);
        if constexpr (std::is_same_v<T, bool>) {
            writer.OnBooleanScalar(item);
        } else if constexpr (std::is_same_v<T, TString> || std::is_same_v<T, TStringBuf>) {
            writer.OnStringScalar(item);
        } else if constexpr (std::is_same_v<T, i64>) {
            writer.OnInt64Scalar(item);
        } else if constexpr (std::is_same_v<T, ui64>) {
            writer.OnUint64Scalar(item);
        } else if constexpr (std::is_same_v<T, double> || std::is_same_v<T, float>) {
            writer.OnDoubleScalar(item);
        } else {
            static_assert(TDependentFalse<T>, "unsupported type");
        }
    }
}

template <class TState>
void LoadStateEntry(TRawRowHolder& row, const NYT::TNode& node)
{
    TState& state = *reinterpret_cast<TState*>(row.GetData());
    LoadStateItem(state.Key(), KeyColumn, node);
    LoadStateItem(state.Value(), ValueColumn, node);
}

template <class TState>
void SaveStateEntry(::NYson::TYsonWriter& writer, void* rawState, const void* rawTKV)
{
    Y_UNUSED(rawTKV);  // it's ok
    const auto& tkv = *reinterpret_cast<const TState*>(rawState);
    if (tkv.Value() == typename TState::TValue{}) {
        return;
    }

    writer.OnListItem();
    writer.OnBeginMap();
    SaveStateItem(writer, tkv.Key(), KeyColumn);
    SaveStateItem(writer, tkv.Value(), ValueColumn);
    writer.OnEndMap();
}

template <class TState>
TRawRowHolder StateFromKey(const void* rawKey)
{
    const auto& key = *reinterpret_cast<const typename TState::TKey*>(rawKey);
    TRawRowHolder result(MakeRowVtable<TState>());
    TState& state = *reinterpret_cast<TState*>(result.GetData());
    state = TState(key, {});
    return result;
}

template <class TState>
TRawRowHolder StateFromTKV(const void* rawTKV)
{
    const auto& tkv = *reinterpret_cast<const TState*>(rawTKV);
    TRawRowHolder result(MakeRowVtable<TState>());
    TState& state = *reinterpret_cast<TState*>(result.GetData());
    state = tkv;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren::NPrivate::NProfileState

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename TState>
TPState<typename TState::TKey, TState> MakeYtKvPState(const TPipeline& ytPipeline, TString inStatePath, TString outStatePath = {})
{
    using NPrivate::MakeRowVtable;
    using NPrivate::TYtStateVtable;
    using NPrivate::NKvState::LoadStateEntry;
    using NPrivate::NKvState::SaveStateEntry;
    using NPrivate::NKvState::StateFromKey;
    using NPrivate::NKvState::StateFromTKV;

    TYtStateVtable stateVtable;
    stateVtable.StateTKVvtable = MakeRowVtable<TState>();
    stateVtable.LoadState = &LoadStateEntry<TState>;
    stateVtable.SaveState = &SaveStateEntry<TState>;
    stateVtable.StateFromKey = &StateFromKey<TState>;
    stateVtable.StateFromTKV = &StateFromTKV<TState>;
    return MakeYtPState<typename TState::TKey, TState>(ytPipeline, std::move(inStatePath), std::move(outStatePath), std::move(stateVtable));
}

template <typename TKey, typename TValue>
TPState<TKey, TKV<TKey, TValue>> MakeYtKvPState(const TPipeline& ytPipeline, TString inStatePath, TString outStatePath = {})
{
    using TState = TKV<TKey, TValue>;
    return MakeYtKvPState<TState>(ytPipeline, std::move(inStatePath), std::move(outStatePath));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
