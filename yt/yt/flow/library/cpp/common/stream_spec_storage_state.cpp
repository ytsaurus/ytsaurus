#include "stream_spec_storage_state.h"

#include "schema.h"

#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NFlow {

using namespace NTableClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

bool UpdateStreamSpecs(
    const TStreamSpecStorageStatePtr& storageState,
    const THashMap<TStreamId, TStreamSpecPtr>& streams,
    const ITimeProviderPtr& timeProvider)
{
    const auto initialNextStreamSpecId = storageState->NextStreamSpecId;

    for (const auto& [streamId, streamSpec] : streams) {
        auto& specVersions = storageState->StreamSpecs[streamId];

        const auto lastSpecIt = specVersions.rbegin();

        if (lastSpecIt != specVersions.rend() && *lastSpecIt->second == *streamSpec) {
            continue;
        }

        const auto uniqueSeqNo = WaitFor(timeProvider->GenerateGlobalUniqueSeqNo()).ValueOrThrow().UniqueSeqNo;
        const auto specId = TStreamSpecId(uniqueSeqNo.Underlying());

        YT_VERIFY(specId >= storageState->NextStreamSpecId);

        specVersions[specId] = streamSpec;

        storageState->NextStreamSpecId = TStreamSpecId(specId.Underlying() + 1);
    }

    return storageState->NextStreamSpecId != initialNextStreamSpecId;
}

bool UpdateGroupBySchemas(
    const TStreamSpecStorageStatePtr& storageState,
    const THashMap<TComputationId, TComputationSpecPtr>& computations)
{
    bool updated = false;

    EraseNodesIf(storageState->GroupBySchemas, [&] (const auto& item) {
        if (!computations.contains(item.first)) {
            updated = true;
            return true;
        }
        return false;
    });

    for (const auto& [computationId, computationSpec] : computations) {
        auto groupBySchemaIt = storageState->GroupBySchemas.find(computationId);

        if (!groupBySchemaIt.IsEnd() && *groupBySchemaIt->second == *computationSpec->GroupBySchema) {
            continue;
        }

        storageState->GroupBySchemas[computationId] = computationSpec->GroupBySchema;
        updated = true;
    }

    return updated;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TStreamSpecStorageState::Register(TRegistrar registrar)
{
    registrar.Parameter("stream_specs", &TThis::StreamSpecs)
        .Default();

    registrar.Parameter("group_by_schemas", &TThis::GroupBySchemas)
        .Default();

    registrar.Parameter("next_stream_spec_id", &TThis::NextStreamSpecId)
        .Default(TStreamSpecId(0));
}

void UpdateStreamSpecStorageState(
    const TVersionedStreamSpecStorageStatePtr& versionedStorageState,
    const TPipelineSpec& pipelineSpec,
    const ITimeProviderPtr& timeProvider)
{
    YT_VERIFY(versionedStorageState);
    const auto& storageState = versionedStorageState->GetValue();

    bool bumpVersion = false;
    bumpVersion |= UpdateStreamSpecs(storageState, pipelineSpec.Streams, timeProvider);
    bumpVersion |= UpdateGroupBySchemas(storageState, pipelineSpec.Computations);

    if (bumpVersion) {
        versionedStorageState->BumpVersion();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
