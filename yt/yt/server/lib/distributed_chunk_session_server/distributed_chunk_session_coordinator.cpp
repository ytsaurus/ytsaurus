#include "distributed_chunk_session_coordinator.h"

#include "config.h"
#include "private.h"

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NDistributedChunkSessionServer {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NRpc;

using NYT::ToProto;

using NApi::NNative::IConnectionPtr;
using NChunkClient::NProto::TMiscExt;
using NDistributedChunkSessionClient::NProto::TRspPingSession;
using NTableClient::NProto::TDataBlockMeta;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDistributedChunkSessionCoordinatorState,
    (Created)
    (Running)
    (Closed)
);

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionCoordinator
    : public IDistributedChunkSessionCoordinator
{
public:
    TDistributedChunkSessionCoordinator(
        TDistributedChunkSessionServiceConfigPtr config,
        TSessionId sessionId,
        std::vector<TNodeDescriptor> targets,
        IInvokerPtr invoker,
        IConnectionPtr connection)
        : Config_(std::move(config))
        , SessionId_(sessionId)
        , Invoker_(std::move(invoker))
        , SerializedInvoker_(CreateSerializedInvoker(Invoker_))
        , Connection_(std::move(connection))
        , Logger(DistributedChunkSessionServiceLogger().WithTag("(SessionId: %v)", SessionId_))
    {
        Nodes_.reserve(targets.size());

        const auto& channelFactory = Connection_->GetChannelFactory();
        const auto& networks = Connection_->GetNetworks();
        for (int index = 0; index < std::ssize(targets); ++index) {
            // TODO(apollo1321): Add retries.
            auto& target = targets[index];
            auto channel = channelFactory->CreateChannel(target.GetAddressOrThrow(networks));
            Nodes_.emplace_back(std::move(target), std::move(channel));
        }
    }

    TFuture<void> StartSession() final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();
        YT_VERIFY(State_.exchange(EDistributedChunkSessionCoordinatorState::Running) == EDistributedChunkSessionCoordinatorState::Created);

        YT_LOG_INFO("Starting distributed chunk write session (SessionId: %v)", SessionId_);

        return CloseRequestedPromise_.ToFuture()
            .Apply(BIND([this, weakThis = MakeWeak(this)] {
                auto this_ = weakThis.Lock();
                if (!this_) {
                    return VoidFuture;
                }
                return AllSucceeded(
                    std::vector{QueueHasBeenProcessed_, AllPendingAckProcessedPromise_.ToFuture()});
            }));
    }

    TFuture<void> SendBlocks(
        std::vector<TBlock> blocks,
        std::vector<TDataBlockMeta> blockMetas,
        TMiscExt blocksMiscMeta) final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();
        YT_VERIFY(State_ >= EDistributedChunkSessionCoordinatorState::Running);

        return BIND(
            &TDistributedChunkSessionCoordinator::DoSendBlocks,
            MakeStrong(this),
            Passed(std::move(blocks)),
            Passed(std::move(blockMetas)),
            Passed(std::move(blocksMiscMeta)))
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

    TFuture<TCoordinatorStatus> UpdateStatus(int acknowledgedBlockCount) final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();
        YT_VERIFY(State_ >= EDistributedChunkSessionCoordinatorState::Running);

        return BIND(
            &TDistributedChunkSessionCoordinator::DoUpdateStatus,
            MakeStrong(this))
            .AsyncVia(SerializedInvoker_)
            .Run(acknowledgedBlockCount);
    }

    TFuture<void> Close(bool force) final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return BIND(
            &TDistributedChunkSessionCoordinator::DoClose,
            MakeStrong(this))
            .AsyncVia(SerializedInvoker_)
            .Run(force);
    }

    ~TDistributedChunkSessionCoordinator()
    {
        // Every SendBlocks takes strong ref so it is not possible for any
        // of the queues to be non-empty in the destructor.
        YT_VERIFY(PendingSendQueue_.empty());
        YT_VERIFY(PendingAckQueue_.empty());
    }

private:
    struct TNode
    {
        TNode(TNodeDescriptor descriptor, IChannelPtr channel)
            : Descriptor(std::move(descriptor))
            , Channel(std::move(channel))
        { }

        TNodeDescriptor Descriptor;
        IChannelPtr Channel;
    };

    struct TPendingBlocks
        : public TRefCounted
    {
        TPendingBlocks(
            int blockIndexBegin,
            int blockIndexEnd,
            TMiscExt blocksMiscMeta,
            std::vector<TBlock> blocks,
            std::vector<TDataBlockMeta> blockMetas)
            : BlockIndexBegin(blockIndexBegin)
            , BlockIndexEnd(blockIndexEnd)
            , BlocksMiscMeta(std::move(blocksMiscMeta))
            , Blocks(std::move(blocks))
            , BlockMetas(std::move(blockMetas))
        { }

        int BlockIndexBegin = 0;
        int BlockIndexEnd = 0;

        TMiscExt BlocksMiscMeta;

        std::vector<TBlock> Blocks;
        std::vector<TDataBlockMeta> BlockMetas;

        const TPromise<void> WriteFinishedPromise = NewPromise<void>();
    };

    using TPendingBlocksPtr = TIntrusivePtr<TPendingBlocks>;

    const TDistributedChunkSessionServiceConfigPtr Config_;
    const TSessionId SessionId_;

    const IInvokerPtr Invoker_;
    const IInvokerPtr SerializedInvoker_;

    const IConnectionPtr Connection_;

    const TLogger Logger;

    std::vector<TNode> Nodes_;

    std::atomic<EDistributedChunkSessionCoordinatorState> State_ = EDistributedChunkSessionCoordinatorState::Created;

    int NextBlockIndex_ = 0;

    const TPromise<void> CloseRequestedPromise_ = NewPromise<void>();

    std::queue<TPendingBlocksPtr> PendingSendQueue_;
    std::queue<TPendingBlocksPtr> PendingAckQueue_;

    int WrittenBlockCount_ = 0;

    TMiscExt ChunkMiscMeta_;
    std::vector<TDataBlockMeta> DataBlocksMeta_;

    TFuture<void> QueueHasBeenProcessed_ = VoidFuture;
    const TPromise<void> AllPendingAckProcessedPromise_ = NewPromise<void>();

    TFuture<void> DoSendBlocks(
        std::vector<TBlock> blocks,
        std::vector<TDataBlockMeta> blockMetas,
        TMiscExt blocksMiscMeta)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);
        YT_VERIFY(State_ != EDistributedChunkSessionCoordinatorState::Created);
        YT_VERIFY(blocks.size() == blockMetas.size());
        THROW_ERROR_EXCEPTION_IF(State_ != EDistributedChunkSessionCoordinatorState::Running, MakeSessionClosedError());

        TForbidContextSwitchGuard contextSwitchGuard;

        auto pendingBlocks = New<TPendingBlocks>(
            NextBlockIndex_,
            NextBlockIndex_ + std::size(blocks),
            std::move(blocksMiscMeta),
            std::move(blocks),
            std::move(blockMetas));

        NextBlockIndex_ = pendingBlocks->BlockIndexEnd;

        YT_LOG_DEBUG("Putting blocks in queue (BlockIndexBegin: %v, BlockIndexEnd: %v)",
            pendingBlocks->BlockIndexBegin,
            pendingBlocks->BlockIndexEnd);

        auto writeFinishedFuture = pendingBlocks->WriteFinishedPromise.ToFuture();

        PendingSendQueue_.push(std::move(pendingBlocks));

        if (QueueHasBeenProcessed_.IsSet()) {
            // It is essential that ProcessQueue and DoSendBlocks be executed
            // within SerializedInvoker_ for Close() to work correctly.
            QueueHasBeenProcessed_ = BIND(
                &TDistributedChunkSessionCoordinator::ProcessQueue,
                MakeStrong(this))
                .AsyncVia(SerializedInvoker_)
                .Run();
        }

        return writeFinishedFuture;
    }

    void ProcessQueue()
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);
        YT_VERIFY(!QueueHasBeenProcessed_.IsSet());

        while (!CloseRequestedPromise_.IsSet() && !PendingSendQueue_.empty()) {
            auto blocksToSend = PendingSendQueue_.front();
            try {
                SendBlocksOnNodes(blocksToSend.get());
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Coordinator failed to send blocks (BlockIndexBegin: %v, BlockIndexEnd: %v)",
                    blocksToSend->BlockIndexBegin,
                    blocksToSend->BlockIndexEnd);
                DoClose(/*force*/ false);
            }
            if (CloseRequestedPromise_.IsSet()) {
                YT_VERIFY(PendingSendQueue_.empty());
                break;
            }
            PendingSendQueue_.pop();

            WrittenBlockCount_ = blocksToSend->BlockIndexEnd;
            ChunkMiscMeta_.set_row_count(ChunkMiscMeta_.row_count() + blocksToSend->BlocksMiscMeta.row_count());
            ChunkMiscMeta_.set_data_weight(ChunkMiscMeta_.data_weight() + blocksToSend->BlocksMiscMeta.data_weight());
            ChunkMiscMeta_.set_compressed_data_size(ChunkMiscMeta_.data_weight());
            ChunkMiscMeta_.set_uncompressed_data_size(ChunkMiscMeta_.data_weight());

            for (auto& block : blocksToSend->BlockMetas) {
                YT_VERIFY(block.chunk_row_count() == 0);
                YT_VERIFY(block.block_index() == 0);

                if (DataBlocksMeta_.empty()) {
                    block.set_chunk_row_count(block.row_count());
                } else {
                    const auto& previousBlock = DataBlocksMeta_.back();
                    block.set_chunk_row_count(block.row_count() + previousBlock.chunk_row_count());
                    block.set_block_index(DataBlocksMeta_.size());
                }
                DataBlocksMeta_.push_back(std::move(block));
            }

            PendingAckQueue_.push(std::move(blocksToSend));
        }
    }

    void SendBlocksOnNodes(TPendingBlocks* blocks)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        YT_LOG_DEBUG("Start writing blocks (BlockIndexBegin: %v, BlockIndexEnd: %v)",
            blocks->BlockIndexBegin,
            blocks->BlockIndexEnd);

        std::vector<TFuture<TDataNodeServiceProxy::TRspPutBlocksPtr>> putBlocksFutures;
        putBlocksFutures.reserve(Nodes_.size());

        // TODO(apollo1321): Add throttling.
        for (const auto& node : Nodes_) {
            TDataNodeServiceProxy proxy(node.Channel);
            proxy.SetDefaultTimeout(Config_->DataNodeRpcTimeout);

            auto req = proxy.PutBlocks();
            req->SetResponseHeavy(true);
            req->SetMultiplexingBand(EMultiplexingBand::Heavy);
            ToProto(req->mutable_session_id(), SessionId_);
            req->set_first_block_index(blocks->BlockIndexBegin);
            req->set_flush_blocks(true);

            SetRpcAttachedBlocks(req, blocks->Blocks);

            putBlocksFutures.push_back(req->Invoke());
        }

        auto allSetFuture = AllSucceeded(std::move(putBlocksFutures))
            .ApplyUnique(BIND([&] (std::vector<TDataNodeServiceProxy::TRspPutBlocksPtr>&& responses) {
                YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

                // Memory can be freed now.
                blocks->Blocks.clear();

                bool closeDemanded = std::any_of(responses.begin(), responses.end(), [] (const auto& response) {
                    return response->close_demanded();
                });

                if (closeDemanded) {
                    DoClose(/*force*/ false);
                }
            }).AsyncVia(SerializedInvoker_));

        WaitFor(AnySet(
            std::vector{CloseRequestedPromise_.ToFuture(), std::move(allSetFuture)},
            TFutureCombinerOptions{.CancelInputOnShortcut = false}))
            .ThrowOnError();
    }

    void DoClose(bool force)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        YT_LOG_INFO("Closing coordinator (Force: %v, PendingSendQueueSize: %v, PendingAckQueueSize: %v)",
            force,
            PendingSendQueue_.size(),
            PendingAckQueue_.size());

        TForbidContextSwitchGuard contextSwitchGuard;

        State_ = EDistributedChunkSessionCoordinatorState::Closed;
        CloseRequestedPromise_.TrySet();

        while (!PendingSendQueue_.empty()) {
            PendingSendQueue_.back()->WriteFinishedPromise.Set(MakeSessionClosedError());
            PendingSendQueue_.pop();
        }

        if (force) {
            while (!PendingAckQueue_.empty()) {
                PendingAckQueue_.back()->WriteFinishedPromise.Set(MakeSessionClosedError());
                PendingAckQueue_.pop();
            }
        }

        if (PendingAckQueue_.empty()) {
            AllPendingAckProcessedPromise_.TrySet();
        }
    }

    TCoordinatorStatus DoUpdateStatus(int acknowledgedBlockCount)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);
        YT_LOG_DEBUG("Updating coordinator status (AcknowledgedBlockCount: %v, CloseRequested: %v, WrittenBlockCount: %v)",
             acknowledgedBlockCount,
             CloseRequestedPromise_.IsSet(),
             WrittenBlockCount_);

        TForbidContextSwitchGuard contextSwitchGuard;

        while (!PendingAckQueue_.empty() &&
                PendingAckQueue_.front()->BlockIndexEnd <= acknowledgedBlockCount)
        {
            PendingAckQueue_.front()->WriteFinishedPromise.Set();
            PendingAckQueue_.pop();
        }

        if (CloseRequestedPromise_.IsSet() && PendingAckQueue_.empty()) {
            AllPendingAckProcessedPromise_.TrySet();
        }

        YT_VERIFY(std::ssize(DataBlocksMeta_) == WrittenBlockCount_);
        YT_VERIFY(std::ssize(DataBlocksMeta_) >= acknowledgedBlockCount);

        std::vector<TDataBlockMeta> blockMetas;
        blockMetas.reserve(WrittenBlockCount_ - acknowledgedBlockCount);
        for (int index = acknowledgedBlockCount; index < WrittenBlockCount_; ++index) {
            blockMetas.push_back(DataBlocksMeta_[index]);
        }

        return TCoordinatorStatus{
            .CloseDemanded = CloseRequestedPromise_.IsSet(),
            .WrittenBlockCount = WrittenBlockCount_,
            .ChunkMiscMeta = ChunkMiscMeta_,
            .DataBlockMetas = std::move(blockMetas),
        };
    }

    TError MakeSessionClosedError() const
    {
        return TError("Distributed chunk write session was closed")
            << TErrorAttribute("session_id", ToString(SessionId_));
    }
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(TRspPingSession* proto, const TCoordinatorStatus& status)
{
    proto->set_close_demanded(status.CloseDemanded);
    proto->set_written_block_count(status.WrittenBlockCount);
    *proto->mutable_chunk_misc_meta() = status.ChunkMiscMeta;
    ToProto(proto->mutable_data_block_metas(), status.DataBlockMetas);
}

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionCoordinatorPtr CreateDistributedChunkSessionCoordinator(
    TDistributedChunkSessionServiceConfigPtr config,
    TSessionId sessionId,
    std::vector<TNodeDescriptor> targets,
    IInvokerPtr invoker,
    IConnectionPtr connection)
{
    return New<TDistributedChunkSessionCoordinator>(
        std::move(config),
        sessionId,
        std::move(targets),
        std::move(invoker),
        std::move(connection));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionServer
