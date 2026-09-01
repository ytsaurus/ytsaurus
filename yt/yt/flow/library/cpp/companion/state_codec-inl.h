#pragma once

#ifndef STATE_CODEC_INL_H_
    #error "Direct inclusion of this file is not allowed, include state_codec.h"
    // For the sake of sane code completion.
    #include "state_codec.h"
#endif

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/misc/cast.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

constexpr auto IsEmptyStatePayload = [] (const auto& statePayload) {
    if constexpr (requires { statePayload.Empty(); }) {
        return statePayload.Empty();
    } else if constexpr (requires { !statePayload; }) {
        return !statePayload;
    } else {
        return statePayload.empty();
    }
};

//! The payload's single boundary copy into the wire message; ref payloads
//! are pre-encoded bytes, other payload types encode via their ToProto.
inline void SetProtoStatePayload(TProtobufString* protoPayload, const TSharedRef& state)
{
    protoPayload->assign(state.Begin(), state.Size());
}

template <typename TStatePayload>
void SetProtoStatePayload(TProtobufString* protoPayload, const TStatePayload& state)
{
    *protoPayload = NYT::ToProto<TProtobufString>(state);
}

//! The payload's exit from the wire message. A ref payload takes at most one
//! buffer copy (none when TString is refcounted) and travels shared past it.
template <typename TStatePayload>
TStatePayload GetProtoStatePayload(const TProtobufString& protoPayload)
{
    if constexpr (std::is_same_v<TStatePayload, TSharedRef>) {
        return TSharedRef::FromString(TString(protoPayload));
    } else {
        return NYT::FromProto<TStatePayload>(protoPayload);
    }
}

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
    // The default format is left unset so the wire bytes of existing pipelines
    // stay unchanged.
    if (state.Format != EStateFormat::SimpleRow) {
        protoState->set_format(ToUnderlying(state.Format));
    }
    if (!state.ProtoType.empty()) {
        protoState->set_proto_type(NYT::ToProto<TProtobufString>(state.ProtoType));
    }
    for (const auto& item : state.StateItems) {
        // An empty payload is legitimate for a proto state: a message whose
        // fields are all defaults serializes to zero bytes.
        if (direction == EStateDirection::Response &&
            !item.Reset &&
            state.Format != EStateFormat::Proto &&
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
            NDetail::SetProtoStatePayload(protoItem->mutable_state(), item.State);
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
    holder.Format = CheckedEnumCast<EStateFormat>(protoState.format());
    holder.ProtoType = NYT::FromProto<std::string>(protoState.proto_type());
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
            .State = NDetail::GetProtoStatePayload<TStatePayload>(protoItem.state()),
        };
        // See SerializeStateHolder: a proto state may legitimately be empty.
        if (direction == EStateDirection::Response &&
            !item.Reset &&
            holder.Format != EStateFormat::Proto &&
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
