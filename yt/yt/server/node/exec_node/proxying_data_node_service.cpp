#include "proxying_data_node_service.h"

#include "bootstrap.h"
#include "private.h"
#include "job_input_cache.h"

#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NExecNode {

using namespace NRpc;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

class TProxyingDataNodeService
    : public TServiceBase
{
public:
    explicit TProxyingDataNodeService(
        NExecNode::IBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetStorageLightInvoker(),
            TDataNodeServiceProxy::GetDescriptor(),
            ExecNodeLogger(),
            ProxyingDataNodeServiceRealmId,
            bootstrap->GetNativeAuthenticator())
        , Config_(bootstrap->GetConfig()->ExecNode)
        , DynamicConfigManager_(bootstrap->GetDynamicConfigManager())
        , Bootstrap_(bootstrap)
        , JobInputCache_(bootstrap->GetJobInputCache())
    {
        YT_VERIFY(Bootstrap_);
        YT_VERIFY(Config_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(ProbeBlockSet));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockSet));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockRange));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkMeta));
    }

private:
    const TExecNodeConfigPtr Config_;
    const TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    NExecNode::IBootstrap* const Bootstrap_;
    const IJobInputCachePtr JobInputCache_;

    TExecNodeDynamicConfigPtr GetDynamicConfig() const
    {
        return DynamicConfigManager_->GetConfig()->ExecNode;
    }

    double GetFallbackTimeoutFraction() const
    {
        return DynamicConfigManager_->GetConfig()->ExecNode->JobInputCache->FallbackTimeoutFraction;
    }

    template <class T, class TContext>
    TFuture<T> WrapWithTimeout(
        TFuture<T> future,
        const TIntrusivePtr<TContext>& context,
        TCallback<T()> fallbackValue)
    {
        if (context->GetTimeout() && context->GetStartTime()) {
            auto deadline = *context->GetStartTime() + *context->GetTimeout() * GetFallbackTimeoutFraction();
            return AnySet<T>({
                future,
                TDelayedExecutor::MakeDelayed(std::max(deadline - TInstant::Now(), TDuration::Zero()))
                    .Apply(fallbackValue)
            });
        } else {
            return future;
        }
    }

    template <class TContext, class TResponse>
    void AttachBlocksToResponse(
        const TIntrusivePtr<TContext>& context,
        TResponse* response,
        const std::vector<NChunkClient::TBlock>& blocks) const
    {
        auto hasCompleteChunk = true;
        response->set_has_complete_chunk(hasCompleteChunk);

        SetRpcAttachedBlocks(response, blocks);

        auto blocksSize = GetByteSize(response->Attachments());

        auto chunkReaderStatistics = New<TChunkReaderStatistics>();
        chunkReaderStatistics->DataBytesReadFromCache.fetch_add(
            blocksSize,
            std::memory_order::relaxed);

        ToProto(response->mutable_chunk_reader_statistics(), chunkReaderStatistics);

        int blocksWithData = std::count_if(
            response->Attachments().begin(),
            response->Attachments().end(),
            [] (const auto& block) -> bool { return static_cast<bool>(block); });

        context->SetResponseInfo(
            "HasCompleteChunk: %v, BlocksWithData: %v, BlocksSize: %v",
            hasCompleteChunk,
            blocksWithData,
            blocksSize);
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, ProbeBlockSet)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto blockIndexes = FromProto<std::vector<int>>(request->block_indexes());
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);

        context->SetRequestInfo(
            "BlockIds: %v:%v, Workload: %v",
            chunkId,
            MakeShrunkFormattableView(blockIndexes, TDefaultFormatter(), 3),
            workloadDescriptor);

        bool hasCompleteChunk = true;
        response->set_has_complete_chunk(hasCompleteChunk);

        if (JobInputCache_->IsBlockCacheMemoryLimitExceeded()) {
            response->set_net_throttling(true);
        }

        context->SetResponseInfo(
            "HasCompleteChunk: %v",
            hasCompleteChunk);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockSet)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto blockIndexes = FromProto<std::vector<int>>(request->block_indexes());
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);
        auto readSessionId = request->has_read_session_id()
            ? FromProto<TReadSessionId>(request->read_session_id())
            : TReadSessionId{};

        context->SetRequestInfo(
            "BlockIds: %v:%v, Workload: %v",
            chunkId,
            MakeShrunkFormattableView(blockIndexes, TDefaultFormatter(), 3),
            workloadDescriptor);

        if (JobInputCache_->IsBlockCacheMemoryLimitExceeded()) {
            response->set_has_complete_chunk(true);
            response->set_net_throttling(true);
            context->SetResponseInfo(
                "HasCompleteChunk: %v, NetThrottling: %v",
                true,
                true);
            context->Reply();
            return;
        }

        IChunkReader::TReadBlocksOptions jobInputCacheOptions;
        jobInputCacheOptions.ClientOptions.WorkloadDescriptor = workloadDescriptor;
        jobInputCacheOptions.ClientOptions.ReadSessionId = readSessionId;

        auto result = JobInputCache_->ReadBlocks(chunkId, blockIndexes, jobInputCacheOptions);
        auto withTimeout = WrapWithTimeout(
            result,
            context,
            BIND([size = blockIndexes.size()] {
                return std::vector<TBlock>(size);
            }));
        auto responseFuture = withTimeout
            .Apply(BIND(&TProxyingDataNodeService::AttachBlocksToResponse<TCtxGetBlockSet, TRspGetBlockSet>, MakeStrong(this), context, response));
        context->ReplyFrom(responseFuture);
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockRange)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        int firstBlockIndex = request->first_block_index();
        int blockCount = request->block_count();
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);
        auto readSessionId = request->has_read_session_id()
            ? FromProto<TReadSessionId>(request->read_session_id())
            : TReadSessionId{};

        context->SetRequestInfo(
            "BlockIds: %v:%v-%v, Workload: %v",
            chunkId,
            firstBlockIndex,
            firstBlockIndex + blockCount - 1,
            workloadDescriptor);

        if (JobInputCache_->IsBlockCacheMemoryLimitExceeded()) {
            response->set_has_complete_chunk(true);
            response->set_net_throttling(true);
            context->SetResponseInfo(
                "HasCompleteChunk: %v, NetThrottling: %v",
                true,
                true);
            context->Reply();
            return;
        }

        IChunkReader::TReadBlocksOptions jobInputCacheOptions;
        jobInputCacheOptions.ClientOptions.WorkloadDescriptor = workloadDescriptor;
        jobInputCacheOptions.ClientOptions.ReadSessionId = readSessionId;

        auto result = JobInputCache_->ReadBlocks(
            chunkId,
            firstBlockIndex,
            blockCount,
            jobInputCacheOptions);
        auto withTimeout = WrapWithTimeout(
            result,
            context,
            BIND([size = blockCount] {
                return std::vector<TBlock>(size);
            }));
        auto responseFuture = withTimeout
            .Apply(BIND(&TProxyingDataNodeService::AttachBlocksToResponse<TCtxGetBlockRange, TRspGetBlockRange>, MakeStrong(this), context, response));
        context->ReplyFrom(responseFuture);
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkMeta)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto partitionTag = request->has_partition_tag()
            ? std::make_optional(request->partition_tag())
            : std::nullopt;
        auto extensionTags = request->all_extension_tags()
            ? std::nullopt
            : std::make_optional(FromProto<std::vector<int>>(request->extension_tags()));
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);

        context->SetRequestInfo(
            "ChunkId: %v, ExtensionTags: %v, PartitionTag: %v, Workload: %v",
            chunkId,
            extensionTags,
            partitionTag,
            workloadDescriptor);

        TClientChunkReadOptions jobInputCacheOptions;
        jobInputCacheOptions.WorkloadDescriptor = workloadDescriptor;

        auto chunkMetaFuture = JobInputCache_->GetChunkMeta(chunkId, jobInputCacheOptions, partitionTag, extensionTags);
        auto withTimeout = WrapWithTimeout(
            chunkMetaFuture,
            context,
            BIND([] {
                return TRefCountedChunkMetaPtr(nullptr);
            }));
        auto responseFuture = withTimeout
            .Apply(BIND([=] (const TRefCountedChunkMetaPtr& meta) {
                if (!meta) {
                    response->set_net_throttling(true);
                    return;
                }

                {
                    // Although it it highly unlikely that job proxy doesn't support some
                    // of the features supported by exec node, we keep this check for consistency.
                    NChunkClient::EChunkFeatures chunkFeatures = FromProto<NChunkClient::EChunkFeatures>(meta->features());
                    NChunkClient::EChunkFeatures supportedChunkFeatures = FromProto<NChunkClient::EChunkFeatures>(request->supported_chunk_features());
                    ValidateChunkFeatures(chunkId, chunkFeatures, supportedChunkFeatures);
                }

                *response->mutable_chunk_meta() = static_cast<NChunkClient::NProto::TChunkMeta>(*meta);

                auto chunkReaderStatistics = New<TChunkReaderStatistics>();
                chunkReaderStatistics->DataBytesReadFromCache = meta->GetCachedSize();

                ToProto(response->mutable_chunk_reader_statistics(), chunkReaderStatistics);

                // ReplicationReader doesn't use this field, other scenarios (such as reading journal chunks) do not require proxying.
                ToProto(response->mutable_location_uuid(), EmptyChunkLocationUuid);
            })
            .AsyncVia(Bootstrap_->GetStorageHeavyInvoker()));
        context->ReplyFrom(responseFuture);
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateProxyingDataNodeService(NExecNode::IBootstrap* bootstrap)
{
    return New<TProxyingDataNodeService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
