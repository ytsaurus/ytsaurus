#include "companion_client_detail.h"
#include "companion_model.h"

#include "private.h"

#include <library/cpp/iterator/concatenate.h>
#include <yt/yt/flow/library/cpp/common/external_metrics_reporter.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/visit.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <library/cpp/yt/yson_string/convert.h>

namespace NYT::NFlow::NCompanion {

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TCompanionComputationInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("computation_id", &TThis::ComputationId);
    registrar.Parameter("computation_type", &TThis::CompanionComputationType)
        .Default();
}

void TCompanionInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("computations", &TThis::Computations)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

template <typename TRequestPtr>
void SetupBasicRequestFields(
    TRequestPtr request,
    const TComputationId& computationId,
    const TJobId& jobId)
{
    request->set_computation_id(ToProto<TProtobufString>(computationId));
    ToProto(request->mutable_job_id(), jobId);
    ToProto(request->mutable_request_id(), TGuid::Create());
}

template <typename TRequestPtr>
void AddStreamsToRequest(
    TRequestPtr request,
    const TStreamSpecsPtr& streamSpecs,
    const auto& streamIds)
{
    for (const auto& streamId : streamIds) {
        auto* addStreamPtr = request->add_streams();
        addStreamPtr->set_stream_id(ToProto<TProtobufString>(streamId));
        addStreamPtr->set_stream_spec_id(ToProto(streamSpecs->GetLastSpecId(streamId)));
        addStreamPtr->set_schema(ToProto(NYson::ConvertToYsonString(streamSpecs->GetSchema(streamId))));
    }
}

template <typename TRequestPtr>
void AddSpecsToRequest(
    TRequestPtr request,
    const TComputationSpecPtr& computationSpec,
    const TDynamicComputationSpecPtr& dynamicComputationSpec)
{
    request->set_spec(ToProto(NYson::ConvertToYsonString(computationSpec)));
    request->set_dynamic_spec(ToProto(NYson::ConvertToYsonString(dynamicComputationSpec)));
}

template <typename TMutableStatesPtr, typename TStatePayload>
void AddStatesToRequest(
    TMutableStatesPtr mutableStates,
    const THashMap<std::string, TStateHolder<TStatePayload>>& States)
{
    for (const auto& [stateName, state] : States) {
        auto addStatePtr = mutableStates->Add();
        addStatePtr->set_name(ToProto<TProtobufString>(stateName));
        if (state.Schema) {
            addStatePtr->set_schema(ToProto(NYson::ConvertToYsonString(state.Schema)));
        }
        for (const auto& stateItem : state.StateItems) {
            auto addStateItemPtr = addStatePtr->add_stateitems();
            ToProto(addStateItemPtr->mutable_key(), stateItem.Key);
            addStateItemPtr->set_state(ToProto<TProtobufString>(stateItem.State));
            addStateItemPtr->set_reset(false);
        }
    }
}

template <typename TProtoStatePayload, typename TStatePayload, typename TStatesPtr>
void ExtractStatesFromRequest(
    std::vector<TStateHolder<TStatePayload>>& states,
    const TStatesPtr& protoStates)
{
    for (const auto& protoState : protoStates) {
        auto stateName = FromProto<TProtobufString>(protoState.name());
        std::vector<TStateItem<TStatePayload>> stateItems;
        for (const auto& protoStateItem : protoState.stateitems()) {
            auto stateItem = TStateItem<TStatePayload>{
                .Key = FromProto<TKey>(protoStateItem.key()),
                .Reset = protoStateItem.reset(),
                .State = FromProto<TProtoStatePayload>(protoStateItem.state()),
            };
            // Validate for empty state.
            constexpr auto isEmpty = [] (auto& state) {
                if constexpr (requires { !state; }) {
                    return !state;
                } else {
                    return state.empty();
                }
            };
            if (!stateItem.Reset && isEmpty(stateItem.State)) {
                THROW_ERROR_EXCEPTION("Empty state value for non-reset state response")
                    << TErrorAttribute("state_name", stateName)
                    << TErrorAttribute("key", stateItem.Key);
            }
            stateItems.push_back(std::move(stateItem));
        }
        states.push_back({
            .StateName = std::move(stateName),
            .StateItems = std::move(stateItems),
        });
    }
}

template <typename TResponse>
void ReportMetrics(
    const TResponse& response,
    const IExternalPerformanceMetricsReporterPtr& reporter)
{
    reporter->AddCpuTime(TDuration::MicroSeconds(response->metrics().cpu_time_ns() / 1000));
    reporter->SetMemoryUsage(static_cast<size_t>(response->metrics().allocated_bytes()));
}

void VerifyRequestResponseIds(const NYT::NProto::TGuid& reqReqId, const NYT::NProto::TGuid& rspReqId)
{
    YT_VERIFY(reqReqId.first() == rspReqId.first());
    YT_VERIFY(reqReqId.second() == rspReqId.second());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TCompanionClient::TCompanionClient(
    const std::string& address,
    const TDuration& timeout,
    const TExponentialBackoffOptions& backoffOptions,
    const IStatusProfilerPtr& statusProfiler)
    : Timeout_(timeout)
    , BackoffOptions_(backoffOptions)
    , Logger(CompanionLogger().WithTag("CompanionClient"))
    , CompanionProxy_(CreateCompanionProxy(address))
    , StatusProfiler_(statusProfiler)
{ }

TCompanionResponsePtr TCompanionClient::DoProcessWithCompanionSync(
    const TCompanionProcessRequestPtr& companionRequest,
    const IExternalPerformanceMetricsReporterPtr& reporter)
{
    auto request = CompanionProxy_.ProcessBatch();
    request->SetTimeout(Timeout_);

    SetupBasicRequestFields(
        request,
        companionRequest->ComputationId,
        companionRequest->JobId);

    // Stream specs for messages serialization.
    auto messageStreamSpecs = companionRequest->GetMessageStreamSpecs();

    // Streams & JobInfo.
    auto inputOutputStreams = Concatenate(
        companionRequest->ComputationSpec->InputStreamIds,
        companionRequest->ComputationSpec->OutputStreamIds);

    if (companionRequest->OverrideStreamSpecs) {
        AddStreamsToRequest(
            request,
            companionRequest->OverrideStreamSpecs,
            inputOutputStreams);
        AddStreamsToRequest(
            request,
            companionRequest->OverrideStreamSpecs,
            GetKeys(companionRequest->ComputationSpec->SourceStreams));
        YT_TLOG_DEBUG("Streams added to process batch request")
            .With("Size", request->streams_size());
    }

    if (companionRequest->SendJobInfo) {
        auto jobInfo = request->mutable_job_info();
        AddSpecsToRequest(
            jobInfo,
            companionRequest->ComputationSpec,
            companionRequest->DynamicComputationSpec);

        AddStreamsToRequest(jobInfo, companionRequest->JobStreamSpecs, inputOutputStreams);
        YT_TLOG_DEBUG("JobInfo added to process batch request");
    }

    // Messages.
    request->mutable_messages()->Reserve(companionRequest->Messages.size());
    for (auto& msg : companionRequest->Messages) {
        auto addMessagePtr = request->add_messages();
        ToProto(addMessagePtr->mutable_message(), *msg, messageStreamSpecs);
        ToProto(addMessagePtr->mutable_key(), msg->Key);
    }
    YT_TLOG_DEBUG("Messages added to process batch request")
        .With("Size", companionRequest->Messages.size());

    // Internal states.
    AddStatesToRequest(request->mutable_internal_states(), companionRequest->InternalStates);
    // External states.
    AddStatesToRequest(request->mutable_external_states(), companionRequest->ExternalStates);
    // Read-only joined external states.
    AddStatesToRequest(request->mutable_joined_external_states(), companionRequest->JoinedExternalStates);
    // Timers.
    request->mutable_timers()->Reserve(companionRequest->Timers.size());
    for (const auto& timer : companionRequest->Timers) {
        ToProto(request->add_timers(), *timer);
    }
    YT_TLOG_DEBUG("Timers added to process batch request")
        .With("Size", request->timers_size());

    // Visits.
    request->mutable_visits()->Reserve(companionRequest->Visits.size());
    for (const auto& visit : companionRequest->Visits) {
        ToProto(request->add_visits(), static_cast<const TVisit&>(*visit));
    }
    YT_TLOG_DEBUG("Visits added to process batch request")
        .With("Size", request->visits_size());

    // Watermarks.
    request->mutable_watermarks()->Reserve(companionRequest->Watermarks.size());
    for (const auto& watermark : companionRequest->Watermarks) {
        auto addWatermarkPtr = request->add_watermarks();
        addWatermarkPtr->set_stream_id(ToProto<TProtobufString>(watermark.StreamId));
        addWatermarkPtr->set_watermark(watermark.Watermark.Underlying());
    }
    YT_TLOG_DEBUG("Watermarks added to process batch request")
        .With("Size", request->watermarks_size());

    auto reqReqId = request->request_id();
    // Request itself with retry logic.
    auto responseFuture = ExecuteWithRetry(BIND([request, this_ = MakeStrong(this)] () {
        return request->Invoke();
    }),
        "DoProcess");

    // Future.GetOrCrash called after WaitFor at ExecuteWithRetry function.
    auto response = responseFuture.GetOrCrash().Value();

    VerifyRequestResponseIds(reqReqId, response->request_id());
    // Metrics.
    ReportMetrics(response, reporter);

    auto responseStatus = static_cast<ECompanionResponseStatus>(response->status());
    YT_TLOG_DEBUG("Received process batch response from companion")
        .With("Status", responseStatus);

    auto companionResponse = New<TCompanionResponse>();
    // Status.
    companionResponse->Status = responseStatus;

    // Early return.
    if (responseStatus != ECompanionResponseStatus::Ok) {
        return companionResponse;
    }

    // Message Groups.
    companionResponse->Groups.reserve(response->data().output_size());
    for (const auto& protoGroup : response->data().output()) {
        TCompanionResponseGroup group;
        group.Messages.reserve(protoGroup.messages().size());
        group.ParentIds.reserve(protoGroup.parent_ids().size());
        group.Timers.reserve(protoGroup.timers().size());
        // Messages.
        for (auto& msg : protoGroup.messages()) {
            group.Messages.push_back(FromProto<TMessage>(msg, messageStreamSpecs));
        }
        // Distribute flags. Empty means "distribute all".
        if (protoGroup.distribute_size() > 0) {
            group.Distribute.reserve(protoGroup.distribute_size());
            for (auto distribute : protoGroup.distribute()) {
                group.Distribute.push_back(distribute);
            }
        } else {
            group.Distribute.assign(group.Messages.size(), true);
        }
        // ParentIds.
        for (auto& parentId : protoGroup.parent_ids()) {
            group.ParentIds.push_back(FromProto<TMessageId>(parentId));
        }
        // New Timers.
        for (const auto& protoTimer : protoGroup.timers()) {
            group.Timers.push_back({
                .TriggerTimestamp = FromProto<TSystemTimestamp>(protoTimer.trigger_timestamp()),
                .EventTimestamp = FromProto<TSystemTimestamp>(protoTimer.event_timestamp()),
                .StreamId = FromProto<TStreamId>(protoTimer.stream_id()),
            });
        }
        YT_TLOG_DEBUG("Received group")
            .With("MessageSize", group.Messages.size())
            .With("ParentIdsSize", group.ParentIds.size())
            .With("TimersSize", group.Timers.size());
        companionResponse->Groups.push_back(std::move(group));
    }

    // Internal states.
    ExtractStatesFromRequest<TProtobufString>(companionResponse->InternalStates, response->data().internal_states());
    YT_TLOG_DEBUG("Received internal states")
        .With("Size", companionResponse->InternalStates.size());
    // External states.
    ExtractStatesFromRequest<TPayload>(companionResponse->ExternalStates, response->data().external_states());
    YT_TLOG_DEBUG("Received external states")
        .With("Size", companionResponse->ExternalStates.size());

    return companionResponse;
}

////////////////////////////////////////////////////////////////////////////////

TCompanionInfoPtr TCompanionClient::GetCompanionInfo()
{
    auto request = CompanionProxy_.CompanionInfo();
    request->SetTimeout(Timeout_);
    auto responseFuture = ExecuteWithRetry(BIND([request, this_ = MakeStrong(this)] () {
        return request->Invoke();
    }),
        "GetCompanionInfo");
    // Future.GetOrCrash called after WaitFor at ExecuteWithRetry function.
    auto response = responseFuture.GetOrCrash().Value();
    auto responseStatus = static_cast<ECompanionResponseStatus>(response->status());

    auto ysonPayload = NYson::TYsonString(FromProto<TProtobufString>(response->payload()));
    YT_TLOG_DEBUG("Received companion info response from companion")
        .With("Status", responseStatus)
        .With("YsonPayload", ysonPayload);
    TCompanionInfoPtr result = NYT::NYTree::ConvertTo<TCompanionInfoPtr>(ysonPayload);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TCompanionPutJobResponsePtr TCompanionClient::PutJob(
    const TCompanionPutJobRequestPtr& companionRequest,
    const IExternalPerformanceMetricsReporterPtr& reporter)
{
    auto request = CompanionProxy_.PutJob();
    request->SetTimeout(Timeout_);
    SetupBasicRequestFields(
        request,
        companionRequest->ComputationId,
        companionRequest->JobId);
    auto jobInfo = request->mutable_job_info();
    AddSpecsToRequest(jobInfo,
        companionRequest->ComputationSpec,
        companionRequest->DynamicComputationSpec);
    // Important: source streams are not serialized here.
    AddStreamsToRequest(
        jobInfo,
        companionRequest->JobStreamSpecs,
        Concatenate(
            companionRequest->ComputationSpec->InputStreamIds,
            companionRequest->ComputationSpec->OutputStreamIds));
    auto responseFuture = ExecuteWithRetry(BIND([request, this_ = MakeStrong(this)] () {
        return request->Invoke();
    }),
        "PutJob");
    // Future.GetOrCrash called after WaitFor at ExecuteWithRetry function.
    auto response = responseFuture.GetOrCrash().Value();
    auto responseStatus = static_cast<ECompanionResponseStatus>(response->status());
    YT_TLOG_DEBUG("Received put job response from companion")
        .With("Status", responseStatus);
    auto putJobResponse = New<TCompanionPutJobResponse>();
    putJobResponse->Status = responseStatus;
    // Metrics.
    ReportMetrics(response, reporter);
    return putJobResponse;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TResponse>
TFuture<TResponse> TCompanionClient::ExecuteWithRetry(
    const TCallback<TFuture<TResponse>()>& callback,
    const std::string& operationName)
{
    auto backoffStrategy = TBackoffStrategy(BackoffOptions_);
    auto errorState = StatusProfiler_->ErrorState(Format("/operations/%v", operationName));
    while (true) {
        auto future = callback();
        auto resultOrError = NConcurrency::WaitFor(future);
        if (resultOrError.IsOK()) {
            return future;
        }
        errorState->SetError(resultOrError);
        if (!backoffStrategy.Next()) {
            resultOrError.ThrowOnError();
        }
        auto backoff = backoffStrategy.GetBackoff();
        YT_TLOG_WARNING("Operation failed, retrying")
            .With("Operation", operationName)
            .With("Attempt", backoffStrategy.GetInvocationIndex())
            .With("SleepDuration", backoff)
            .With(resultOrError);
        NConcurrency::TDelayedExecutor::WaitForDuration(backoff);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
