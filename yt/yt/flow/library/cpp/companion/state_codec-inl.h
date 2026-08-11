#pragma once

#ifndef STATE_CODEC_INL_H_
    #error "Direct inclusion of this file is not allowed, include state_codec.h"
    // For the sake of sane code completion.
    #include "state_codec.h"
#endif

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

constexpr auto IsEmptyStatePayload = [] (const auto& statePayload) {
    if constexpr (requires { !statePayload; }) {
        return !statePayload;
    } else {
        return statePayload.empty();
    }
};

} // namespace NDetail

template <typename TStatePayload, typename TProtoState>
void SerializeStateHolder(
    TProtoState* protoState,
    const TStateHolder<TStatePayload>& state,
    EStateDirection direction)
{
    protoState->set_name(NYT::ToProto<TProtobufString>(state.StateName));
    if (state.Schema) {
        protoState->set_schema(NYT::ToProto(NYson::ConvertToYsonString(state.Schema)));
    }
    for (const auto& item : state.StateItems) {
        if (direction == EStateDirection::Response &&
            !item.Reset &&
            NDetail::IsEmptyStatePayload(item.State))
        {
            THROW_ERROR_EXCEPTION("Empty state value for non-reset state %Qv",
                state.StateName)
                .With("key", item.Key);
        }
        auto* protoItem = protoState->add_stateitems();
        NYT::ToProto(protoItem->mutable_key(), item.Key);
        protoItem->set_reset(item.Reset);
        if (!item.Reset) {
            protoItem->set_state(NYT::ToProto<TProtobufString>(item.State));
        }
    }
}

template <typename TStatePayload, typename TProtoState>
TStateHolder<TStatePayload> ParseStateHolder(
    const TProtoState& protoState,
    EStateDirection direction)
{
    TStateHolder<TStatePayload> holder;
    holder.StateName = NYT::FromProto<std::string>(protoState.name());
    if (direction == EStateDirection::Request &&
        protoState.has_schema() &&
        !protoState.schema().empty())
    {
        holder.Schema = NYTree::ConvertTo<NTableClient::TTableSchemaPtr>(
            NYson::TYsonStringBuf(protoState.schema()));
    }
    holder.StateItems.reserve(protoState.stateitems_size());
    for (const auto& protoItem : protoState.stateitems()) {
        auto item = TStateItem<TStatePayload>{
            .Key = NYT::FromProto<TKey>(protoItem.key()),
            .Reset = protoItem.reset(),
            .State = NYT::FromProto<TStatePayload>(protoItem.state()),
        };
        if (direction == EStateDirection::Response &&
            !item.Reset &&
            NDetail::IsEmptyStatePayload(item.State))
        {
            THROW_ERROR_EXCEPTION("Empty state value for non-reset state %Qv",
                holder.StateName)
                .With("key", item.Key);
        }
        holder.StateItems.push_back(std::move(item));
    }
    return holder;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
