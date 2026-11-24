#include "sink_test_environment.h"

#include <yt/yt/flow/extensions/ydb/cpp/deferred_publication.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/time_provider.h>

#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NYdbTesting {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

const TStreamId SinkTestStreamId("test");

TSinkTestEnvironment::TSinkTestEnvironment(
    TSinkSpecShape shape,
    bool framing,
    i64 maxRowsPerBatch,
    ui64 jobGeneration,
    bool asyncInvoker)
    : Shape(std::move(shape))
{
    Schema = New<TTableSchema>(std::vector{
        TColumnSchema("payload", EValueType::String),
    });

    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = Schema;
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> specs;
    specs[SinkTestStreamId][TStreamSpecId(1)] = streamSpec;
    StreamSpecStorage = New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(specs),
        New<TTableSchema>(),
        /*evaluatorCache*/ nullptr);

    Context = New<TSinkContext>();
    Context->Logger = TestLogger;
    if (asyncInvoker) {
        SerializedQueue = New<NConcurrency::TActionQueue>("SinkTest");
        SuspendableInvoker = NConcurrency::CreateSuspendableInvoker(SerializedQueue->GetInvoker());
        Context->SerializedInvoker = SuspendableInvoker;
    } else {
        Context->SerializedInvoker = GetSyncInvoker();
    }
    Context->StatusProfiler = CreateSyncStatusProfiler(TestLogger);
    Registry->SetWindowSize(12);
    Context->Profiler = NProfiling::TProfiler(Registry, "/test");
    Context->SinkId = TSinkId("sink");
    Context->JobGeneration = TUniqueSeqNo(jobGeneration);
    Context->Partition = New<TPartition>();
    Context->Partition->PartitionId = TPartitionId(TGuid::Create());
    Context->StreamSpecStorage = StreamSpecStorage;
    Context->SinkSpec = New<TSinkSpec>();
    Context->SinkSpec->SinkClassName = Shape.SinkClassName;
    Context->SinkSpec->InputStreamIds = {SinkTestStreamId};
    for (const auto& [name, value] : Shape.ConnectionParameters) {
        Context->SinkSpec->Parameters->AddChild(name, NYTree::CloneNode(value));
    }
    Context->SinkSpec->Parameters->AddChild("payload_column", NYTree::ConvertToNode("payload"));

    auto resourceContext = New<TResourceContext>();
    resourceContext->ResourceSpec = New<TResourceSpec>();
    resourceContext->ResourceSpec->ResourceClassName = Shape.ResourceClassName;
    resourceContext->Logger = TestLogger;
    auto dynamicResourceContext = New<TDynamicResourceContext>();
    dynamicResourceContext->DynamicResourceSpec = New<TDynamicResourceSpec>();
    Resource = New<TFakeYdbResource>(resourceContext, dynamicResourceContext, WriteState, DeferredClient);
    Context->StaticResources.emplace(YdbDriverDefaultResourceId, Resource);

    DynamicContext = New<TDynamicSinkContext>();
    DynamicContext->DynamicSinkSpec = New<TDynamicSinkSpec>();
    if (framing) {
        Context->SinkSpec->Parameters->AddChild("framing_format", NYTree::ConvertToNode("lenval"));
        DynamicContext->DynamicSinkSpec->Parameters->AddChild(
            "max_rows_per_batch",
            NYTree::ConvertToNode(maxRowsPerBatch));
    }
}

std::string TSinkTestEnvironment::GetExternalPublicationId() const
{
    return Format("ytflow/%v/%v", Context->SinkId, Context->Partition->PartitionId);
}

std::string TSinkTestEnvironment::MakeWriterIdentity(TUniqueSeqNo generation, ui64 preparationAttempt) const
{
    return NDetail::MakeDeferredPublicationWriterIdentity(
        Shape.WriterIdentityType,
        generation,
        preparationAttempt,
        Context->SinkId,
        Context->Partition->PartitionId);
}

TOutputMessageConstPtr TSinkTestEnvironment::MakeMessage(i64 id, std::string payload) const
{
    TMessageBuilder builder(SinkTestStreamId, Schema);
    builder.SetMessageId(TMessageId(LexicographicallySerialize(id)));
    builder.SetSystemTimestamp(TSystemTimestamp(1'700'000'000));
    builder.SetAlignmentTimestamp(TSystemTimestamp(1'700'000'000));
    builder.SetEventTimestamp(TSystemTimestamp(1'700'000'000));
    builder.Payload().Set<std::string>(std::move(payload), "payload");
    return New<TOutputMessage>(builder.Finish(), StreamSpecStorage);
}

void TSinkTestEnvironment::SetJobGeneration(ui64 generation) const
{
    Context->JobGeneration = TUniqueSeqNo(generation);
}

void TSinkTestEnvironment::DrainAsync() const
{
    YT_VERIFY(SuspendableInvoker);
    NConcurrency::WaitFor(BIND([] {
    }).AsyncVia(SuspendableInvoker)
            .Run())
        .ThrowOnError();
}

std::optional<TYdbDeferredPublicationStatePtr> TSinkTestEnvironment::TryGetPersistedDeferredState() const
{
    auto serialized = StateManager->TryGet("/deferred_publication/v0");
    if (!serialized) {
        return std::nullopt;
    }
    return NYTree::ConvertTo<TYdbDeferredPublicationStatePtr>(*serialized);
}

TYdbDeferredPublicationStatePtr TSinkTestEnvironment::GetPersistedDeferredState() const
{
    return TryGetPersistedDeferredState().value_or(New<TYdbDeferredPublicationState>());
}

TYdbPendingPublicationStatePtr TSinkTestEnvironment::GetPersistedDeferredPublication() const
{
    auto state = GetPersistedDeferredState();
    return state->PendingPublications.empty()
        ? New<TYdbPendingPublicationState>()
        : state->PendingPublications.back();
}

ISinkPtr TSinkTestEnvironment::CreateSink() const
{
    return TRegistry::Get()->CreateSink(Context, DynamicContext);
}

void TSinkTestEnvironment::PersistCookie(ui64 id, std::string endpoint, std::string database) const
{
    PersistCookies({{id, std::move(endpoint), std::move(database)}});
}

void TSinkTestEnvironment::PersistCookies(const std::vector<std::tuple<ui64, std::string, std::string>>& cookies) const
{
    auto state = New<TYdbDeferredPublicationState>();
    for (const auto& [id, endpoint, database] : cookies) {
        auto pending = New<TYdbPendingPublicationState>();
        pending->IntPublicationId = id;
        pending->Endpoint = endpoint;
        pending->Database = database;
        state->PendingPublications.push_back(std::move(pending));
    }
    StateManager->Set("/deferred_publication/v0", NYson::ConvertToYsonString(state));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYdbTesting
