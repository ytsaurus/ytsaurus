#include "codec.h"

#include <yt/yt/flow/library/cpp/companion/state_codec.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NCompanionServer {

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <typename TStatePayload, typename TProtoStates>
THashMap<std::string, NCompanion::TStateHolder<TStatePayload>> ParseStates(
    const TProtoStates& protoStates)
{
    THashMap<std::string, NCompanion::TStateHolder<TStatePayload>> states;
    for (const auto& protoState : protoStates) {
        auto holder = NCompanion::ParseStateHolder<TStatePayload>(
            protoState,
            NCompanion::EStateDirection::Request);
        auto stateName = holder.StateName;
        auto [it, inserted] = states.emplace(std::move(stateName), std::move(holder));
        // Malformed wire input must fail the request, not abort the process.
        THROW_ERROR_EXCEPTION_UNLESS(inserted,
            "Duplicate state %Qv in the request",
            it->first);
    }
    return states;
}

template <typename TStatePayload, typename TMutableProtoStates>
void SerializeStates(
    TMutableProtoStates* mutableStates,
    const std::vector<NCompanion::TStateHolder<TStatePayload>>& states)
{
    for (const auto& state : states) {
        NCompanion::SerializeStateHolder(
            mutableStates->Add(),
            state,
            NCompanion::EStateDirection::Response);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TBatchInput ParseProcessBatchRequest(
    const NProto::NCompanion::TReqProcessBatch& request,
    const TStreamSpecsPtr& messageStreamSpecs,
    const NTableClient::TTableSchemaPtr& keySchema)
{
    TBatchInput input;

    input.Messages.reserve(request.messages_size());
    for (const auto& protoExtendedMessage : request.messages()) {
        auto message = FromProto<TMessage>(protoExtendedMessage.message(), messageStreamSpecs);
        input.Messages.push_back(New<TInputMessage>(
            std::move(message),
            FromProto<TKey>(protoExtendedMessage.key())));
    }

    input.Timers.reserve(request.timers_size());
    for (const auto& protoTimer : request.timers()) {
        TTimer timer;
        FromProto(&timer, protoTimer);
        if (!timer.KeySchema) {
            timer.KeySchema = keySchema;
        }
        input.Timers.push_back(New<TInputTimer>(std::move(timer)));
    }

    input.Visits.reserve(request.visits_size());
    for (const auto& protoVisit : request.visits()) {
        TVisit visit;
        FromProto(&visit, protoVisit);
        // The wire TVisit carries no alignment timestamp, so FromProto always
        // leaves it zero; the system timestamp is a stand-in, not a fallback —
        // in-process code sees the worker's real alignment timestamp instead.
        if (visit.AlignmentTimestamp == TSystemTimestamp(0)) {
            visit.AlignmentTimestamp = visit.SystemTimestamp;
        }
        input.Visits.push_back(New<TInputVisit>(std::move(visit)));
    }

    input.InternalStates = ParseStates<std::string>(request.internal_states());
    input.ExternalStates = ParseStates<TPayload>(request.external_states());
    input.JoinedExternalStates = ParseStates<TPayload>(request.joined_external_states());

    for (const auto& protoWatermark : request.watermarks()) {
        input.Watermarks[TStreamId(protoWatermark.stream_id())] =
            TSystemTimestamp(protoWatermark.watermark());
    }

    return input;
}

////////////////////////////////////////////////////////////////////////////////

void SerializeProcessBatchResponse(
    NProto::NCompanion::TResponseData* data,
    const std::vector<TOutputGroup>& groups,
    const std::vector<NCompanion::TStateHolder<std::string>>& internalStates,
    const std::vector<NCompanion::TStateHolder<TPayload>>& externalStates,
    const TStreamSpecsPtr& messageStreamSpecs)
{
    for (const auto& group : groups) {
        THROW_ERROR_EXCEPTION_IF(group.ParentIds.empty(),
            "Output group without parent ids");
        YT_VERIFY(group.Distribute.size() == group.Messages.size());

        auto* protoGroup = data->add_output();
        for (const auto& message : group.Messages) {
            ToProto(protoGroup->add_messages(), message, messageStreamSpecs);
        }
        // Empty distribute list means "distribute all".
        if (std::find(group.Distribute.begin(), group.Distribute.end(), false) !=
            group.Distribute.end())
        {
            for (auto distribute : group.Distribute) {
                protoGroup->add_distribute(distribute);
            }
        }
        for (const auto& timer : group.Timers) {
            auto* protoTimer = protoGroup->add_timers();
            protoTimer->set_trigger_timestamp(timer.TriggerTimestamp.Underlying());
            if (timer.EventTimestamp) {
                protoTimer->set_event_timestamp(timer.EventTimestamp->Underlying());
            }
            if (timer.StreamId) {
                protoTimer->set_stream_id(ToProto<TProtobufString>(*timer.StreamId));
            }
        }
        for (const auto& parentId : group.ParentIds) {
            protoGroup->add_parent_ids(ToProto<TProtobufString>(parentId));
        }
    }

    SerializeStates(data->mutable_internal_states(), internalStates);
    SerializeStates(data->mutable_external_states(), externalStates);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
