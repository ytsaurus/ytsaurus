#include "adapter_test_context.h"

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/flow/library/cpp/common/partition_buffer_state.h>

#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

namespace NYT::NFlow::NTesting {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TStubClientsCache
    : public NClient::NCache::IClientsCache
{
public:
    NApi::IClientPtr GetClient(TStringBuf /*clusterUrl*/) override
    {
        return New<NApi::TMockClient>();
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TComputationContextPtr MakeAdapterTestComputationContext(
    const IInvokerPtr& invoker,
    TComputationSpecPtr spec)
{
    auto context = New<TComputationContext>();
    context->ComputationSpec = std::move(spec);
    context->ClientsCache = New<TStubClientsCache>();
    context->PipelinePath = NYPath::TRichYPath("//pipeline");
    context->PipelinePath.SetCluster("test");
    context->Partition = New<TPartition>();
    // No source stream is configured, so a live (Executing) partition would fail the ctor's
    // "active source is undefined" check; Completed skips that source-binding path entirely.
    context->Partition->State = EPartitionState::Completed;
    context->Job = New<TJob>();
    context->SerializedInvoker = invoker;
    context->PoolInvoker = invoker;
    context->StatusProfiler = CreateSyncStatusProfiler();
    context->DistributedThrottlerControllerChannelProvider = [] {
        return NRpc::IChannelPtr();
    };
    context->PartitionBufferState = CreateDetachedPartitionBufferState({});
    return context;
}

TDynamicComputationContextPtr MakeAdapterTestDynamicComputationContext()
{
    auto dynamicContext = New<TDynamicComputationContext>();
    dynamicContext->DynamicComputationSpec = New<TDynamicComputationSpec>();
    auto partitionSpec = New<TDynamicPartitionSpec>();
    partitionSpec->ComputationPartitionSpec = NYTree::GetEphemeralNodeFactory()->CreateMap();
    dynamicContext->DynamicPartitionSpec = std::move(partitionSpec);
    return dynamicContext;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
