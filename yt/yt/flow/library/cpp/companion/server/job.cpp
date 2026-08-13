#include "job.h"

#include "codec.h"
#include "output_collector.h"
#include "resource_store.h"
#include "runtime_context.h"
#include "runtime_init_context.h"

#include <yt/yt/flow/library/cpp/common/input_context.h>

#include <yt/yt/flow/library/cpp/process_function/host/computation.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/generic/map.h>

namespace NYT::NFlow::NCompanionServer {

using namespace NCompanion;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TStreamSpecsPtr BuildStreamSpecs(
    const google::protobuf::RepeatedPtrField<NProto::NCompanion::TStream>& protoStreams)
{
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> streamSpecs;
    for (const auto& protoStream : protoStreams) {
        auto streamSpec = New<TStreamSpec>();
        streamSpec->Schema = ConvertTo<NTableClient::TTableSchemaPtr>(
            NYson::TYsonStringBuf(protoStream.schema()));
        streamSpecs[TStreamId(protoStream.stream_id())]
                   [TStreamSpecId(protoStream.stream_spec_id())] = std::move(streamSpec);
    }
    return New<TStreamSpecs>(streamSpecs);
}

TStreamSpecsPtr TStreamSpecCache::Resolve(
    const google::protobuf::RepeatedPtrField<NProto::NCompanion::TStream>& protoStreams)
{
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> streamSpecs;
    for (const auto& protoStream : protoStreams) {
        auto key = std::pair(
            TStreamId(protoStream.stream_id()),
            TStreamSpecId(protoStream.stream_spec_id()));
        auto& cached = Specs_[key];
        if (!cached.Spec || cached.SchemaBytes != protoStream.schema()) {
            auto streamSpec = New<TStreamSpec>();
            streamSpec->Schema = ConvertTo<NTableClient::TTableSchemaPtr>(
                NYson::TYsonStringBuf(protoStream.schema()));
            cached = {TString(protoStream.schema()), std::move(streamSpec)};
        }
        streamSpecs[key.first][key.second] = cached.Spec;
    }
    return New<TStreamSpecs>(streamSpecs);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

THashSet<std::string> ExtractInternalStateNames(const TComputationSpecPtr& spec)
{
    THashSet<std::string> result;
    if (!spec->Parameters) {
        return result;
    }
    if (auto child = spec->Parameters->FindChild("internal_states")) {
        for (const auto& name : ConvertTo<std::vector<std::string>>(child)) {
            result.insert(name);
        }
    }
    return result;
}

template <typename TSpecMap>
THashSet<std::string> ExtractKeys(const TSpecMap& specMap)
{
    THashSet<std::string> result;
    for (const auto& [name, spec] : specMap) {
        result.insert(name);
    }
    return result;
}

} // namespace

TJob::TJob(
    TJobId jobId,
    TComputationId computationId,
    const NProto::NCompanion::TJobInfo& jobInfo,
    TResourceStorePtr resourceStore)
    : JobId_(jobId)
    , ComputationId_(std::move(computationId))
    , ResourceStore_(std::move(resourceStore))
    // A companion never evaluates expression columns: stream schemas cannot have them, keys arrive
    // on the wire, and joined-state keys are stripped by TCompanionExternalStateJoiner. This keeps
    // the query engine out of every binary users ship, at the price of ComputeKey() on a computed
    // group-by schema — see TCompanionRuntimeContext.
    , ConverterCache_(CreatePayloadConverterCache(/*evaluatorCache*/ nullptr))
{
    try {
        Spec_ = ConvertTo<TComputationSpecPtr>(NYson::TYsonStringBuf(jobInfo.spec()));
        DynamicSpec_ = ConvertTo<TDynamicComputationSpecPtr>(
            NYson::TYsonStringBuf(jobInfo.dynamic_spec()));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to parse job specs")
            .With("job_id", JobId_)
            .With("computation_id", ComputationId_)
            .With(ex);
    }
    StreamSpecs_ = BuildStreamSpecs(jobInfo.streams());
    CompanionResources_.reserve(jobInfo.companion_resources_size());
    for (const auto& protoReference : jobInfo.companion_resources()) {
        TCompanionResourceInstanceReference reference;
        FromProto(&reference, protoReference);
        CompanionResources_.push_back(std::move(reference));
    }

    InternalStateNames_ = ExtractInternalStateNames(Spec_);
    ExternalStateNames_ = ExtractKeys(Spec_->ExternalStateManagers);
    JoinedStateNames_ = ExtractKeys(Spec_->ExternalStateJoiners);
}

const TJobId& TJob::GetJobId() const
{
    return JobId_;
}

const TComputationId& TJob::GetComputationId() const
{
    return ComputationId_;
}

const TComputationSpecPtr& TJob::GetSpec() const
{
    return Spec_;
}

const TDynamicComputationSpecPtr& TJob::GetDynamicSpec() const
{
    return DynamicSpec_;
}

const TStreamSpecsPtr& TJob::GetStreamSpecs() const
{
    return StreamSpecs_;
}

const THashSet<std::string>& TJob::GetInternalStateNames() const
{
    return InternalStateNames_;
}

const THashSet<std::string>& TJob::GetExternalStateNames() const
{
    return ExternalStateNames_;
}

const THashSet<std::string>& TJob::GetJoinedStateNames() const
{
    return JoinedStateNames_;
}

const std::vector<TCompanionResourceInstanceReference>& TJob::GetCompanionResources() const
{
    return CompanionResources_;
}

////////////////////////////////////////////////////////////////////////////////

bool TJob::EnsureInitialized()
{
    if (Initialized_) {
        return true;
    }

    // A reference can stop matching between the caller's store check and this
    // acquisition when a lifecycle command advances the resource concurrently;
    // report it in-band so the worker heals with a re-init instead of seeing
    // an RPC error.
    auto resources = AcquireRequiredResources();
    if (!resources) {
        return false;
    }

    // Validated with a throw before the host helpers, whose YT_VERIFY is meant
    // for worker-side specs pre-validated at pipeline submission; here the
    // spec is wire input and must fail the request instead.
    THROW_ERROR_EXCEPTION_UNLESS(Spec_->ProcessingFunction,
        "Computation %Qv spec does not name a processing function; "
        "the C++ companion hosts process functions only",
        ComputationId_);

    auto function = CreateProcessFunction(Spec_);
    THROW_ERROR_EXCEPTION_IF(
        ViewProcessFunctionAsSync(Spec_, function),
        "Process function %Qv overrides Sync; "
        "sync process functions are not supported in companions",
        *Spec_->ProcessingFunction);

    THashMap<std::string, TCompanionExternalStateJoinerConfig> joinedStateConfigs;
    for (const auto& [name, joinerSpec] : Spec_->ExternalStateJoiners) {
        const auto& joinOn = joinerSpec->JoinOn;
        joinedStateConfigs.emplace(name, TCompanionExternalStateJoinerConfig{
                .KeySchema = joinOn->KeySchemaOverride ? joinOn->KeySchemaOverride : Spec_->GroupBySchema,
                .ConverterCache = ConverterCache_,
                .KeyProviderStreams = joinOn->KeyProviderStreams,
                .HasKeySchemaOverride = joinOn->KeySchemaOverride != nullptr,
                                         });
    }

    StateStore_ = New<TCompanionStateStore>(
        InternalStateNames_,
        ExternalStateNames_,
        JoinedStateNames_,
        Spec_->GroupBySchema,
        std::move(joinedStateConfigs));

    auto initContext = New<TCompanionRuntimeInitContext>(
        StateStore_,
        Spec_->ProcessingFunctionParameters,
        std::move(*resources));
    function->Init(initContext);

    BatchFunction_ = WrapAsBatch(function);
    RuntimeContext_ = New<TCompanionRuntimeContext>(
        Spec_,
        New<TComputationStreamSpecStorage>(
            StreamSpecs_,
            Spec_->GroupBySchema,
            ConverterCache_),
        Spec_->GroupBySchema,
        ConverterCache_,
        /*throttlerFactory*/ nullptr);
    Initialized_ = true;
    return true;
}

std::optional<THashMap<TResourceId, IResourcePtr>> TJob::AcquireRequiredResources() const
{
    THashMap<TResourceId, IResourcePtr> resources;
    if (!ResourceStore_) {
        return resources;
    }

    for (const auto& reference : CompanionResources_) {
        if (!reference.Alias) {
            continue;
        }
        auto resource = ResourceStore_->FindInitializedResource(reference);
        if (!resource) {
            return std::nullopt;
        }
        resources[*reference.Alias] = std::move(resource);
    }
    return resources;
}

bool TJob::ProcessBatch(
    const NProto::NCompanion::TReqProcessBatch& request,
    NProto::NCompanion::TResponseData* data)
{
    if (!EnsureInitialized()) {
        return false;
    }

    // Source batches override the stream specs; message payloads are encoded
    // against the override when present.
    bool hasStreamOverride = request.streams_size() > 0;
    auto messageStreamSpecs = hasStreamOverride
        ? OverrideSpecCache_.Resolve(request.streams())
        : StreamSpecs_;

    auto input = ParseProcessBatchRequest(request, messageStreamSpecs, Spec_->GroupBySchema);
    StateStore_->LoadBatch(input);

    auto runtimeContext = hasStreamOverride
        ? New<TCompanionRuntimeContext>(
            Spec_,
            New<TComputationStreamSpecStorage>(
                messageStreamSpecs,
                Spec_->GroupBySchema,
                ConverterCache_),
            Spec_->GroupBySchema,
            ConverterCache_,
            /*throttlerFactory*/ nullptr)
        : RuntimeContext_;
    runtimeContext->RefreshEpochState(
        BuildWatermarkState(input.Watermarks),
        DynamicSpec_->ProcessingFunctionParameters);

    auto outputCollector = TGroupingOutputCollector::CreateRoot(
        input.Messages,
        input.Timers,
        input.Visits);

    auto inputContext = New<TInputContext>(input.Messages, input.Timers, input.Visits);
    BatchFunction_->Process(inputContext, outputCollector, runtimeContext);

    std::vector<NCompanion::TStateHolder<std::string>> internalStates;
    std::vector<NCompanion::TStateHolder<TPayload>> externalStates;
    StateStore_->CollectModified(&internalStates, &externalStates);

    SerializeProcessBatchResponse(
        data,
        outputCollector->TakeGroups(),
        internalStates,
        externalStates,
        messageStreamSpecs);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
