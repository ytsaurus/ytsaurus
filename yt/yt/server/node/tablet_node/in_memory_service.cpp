#include "public.h"
#include "private.h"
#include "in_memory_service.h"
#include "in_memory_service_proxy.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/tablet_node/in_memory_manager.h>
#include <yt/yt/server/node/tablet_node/slot_manager.h>

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/lease_manager.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/optional.h>

#include <yt/yt/core/ytalloc/memory_zone.h>

namespace NYT::NTabletNode {

using namespace NRpc;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NTabletClient;

using NNodeTrackerClient::EMemoryCategory;

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TInterceptingBlockCache
{
public:
    TInterceptingBlockCache(EInMemoryMode mode, NClusterNode::TBootstrap* bootstrap)
        : Mode_(mode)
        , Bootstrap_(bootstrap)
    { }

    void PutBlock(
        const TBlockId& id,
        const TBlock& block)
    {
        const auto& memoryTracker = Bootstrap_->GetMemoryUsageTracker();

        if (memoryTracker->IsExceeded(EMemoryCategory::TabletStatic)) {
            Dropped_ = true;
        }

        auto chunkId = id.ChunkId;

        if (Dropped_) {
            auto guard = Guard(SpinLock_);
            if (ChunkIdToData_.erase(chunkId) == 1) {
                YT_LOG_WARNING("Intercepted chunk data dropped due to memory pressure (ChunkId: %v)",
                    chunkId);
            }
            return;
        }

        auto guard = Guard(SpinLock_);

        auto it = ChunkIdToData_.find(chunkId);
        TInMemoryChunkDataPtr data;
        if (it == ChunkIdToData_.end()) {
            data = New<TInMemoryChunkData>();

            data->InMemoryMode = Mode_;
            data->MemoryTrackerGuard = TMemoryUsageTrackerGuard::Acquire(
                Bootstrap_
                    ->GetMemoryUsageTracker()
                    ->WithCategory(EMemoryCategory::TabletStatic),
                0 /*size*/,
                MemoryUsageGranularity);

            YT_LOG_INFO("Intercepted chunk data created (ChunkId: %v, Mode: %v)",
                chunkId,
                Mode_);

            // Replace the old data, if any, by a new one.
            ChunkIdToData_[chunkId] = data;
        } else {
            data = GetOrCrash(ChunkIdToData_, chunkId);
            YT_VERIFY(data->InMemoryMode == Mode_);
        }

        if (std::ssize(data->Blocks) <= id.BlockIndex) {
            ssize_t blockCapacity = std::max(data->Blocks.capacity(), static_cast<size_t>(1));
            while (blockCapacity <= id.BlockIndex) {
                blockCapacity *= 2;
            }
            data->Blocks.reserve(blockCapacity);
            data->Blocks.resize(id.BlockIndex + 1);
        }

        YT_VERIFY(!data->Blocks[id.BlockIndex].Data);
        data->Blocks[id.BlockIndex] = block;
        if (data->MemoryTrackerGuard) {
            data->MemoryTrackerGuard.IncrementSize(block.Size());
        }
        YT_VERIFY(!data->ChunkMeta);
    }

    TInMemoryChunkDataPtr ExtractChunkData(TChunkId chunkId)
    {
        auto guard = Guard(SpinLock_);
        return GetOrCrash(ChunkIdToData_, chunkId);
    }

private:
    const EInMemoryMode Mode_;
    NClusterNode::TBootstrap* const Bootstrap_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);
    THashMap<TChunkId, TInMemoryChunkDataPtr> ChunkIdToData_;
    bool Dropped_ = false;
};

DECLARE_REFCOUNTED_STRUCT(TInMemorySession)

struct TInMemorySession
    : public TRefCounted
    , public TInterceptingBlockCache
{
    TInMemorySession(EInMemoryMode mode, NClusterNode::TBootstrap* bootstrap, TLease lease)
        : TInterceptingBlockCache(mode, bootstrap)
        , Lease(std::move(lease))
    { }

    const TLease Lease;
};

DEFINE_REFCOUNTED_TYPE(TInMemorySession)

////////////////////////////////////////////////////////////////////////////////

class TInMemoryService
    : public TServiceBase
{
public:
    TInMemoryService(
        TInMemoryManagerConfigPtr config,
        NClusterNode::TBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetStorageLightInvoker(),
            TInMemoryServiceProxy::GetDescriptor(),
            TabletNodeLogger)
        , Config_(config)
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PutBlocks));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishSession));
    }

private:
    const TInMemoryManagerConfigPtr Config_;
    NClusterNode::TBootstrap* const Bootstrap_;

    YT_DECLARE_SPINLOCK(TReaderWriterSpinLock, SessionMapLock_);
    THashMap<TInMemorySessionId, TInMemorySessionPtr> SessionMap_;

    DECLARE_RPC_SERVICE_METHOD(NTabletNode::NProto, StartSession)
    {
        auto inMemoryMode = FromProto<EInMemoryMode>(request->in_memory_mode());

        context->SetRequestInfo("InMemoryMode: %v", inMemoryMode);

        auto sessionId = TInMemorySessionId::Create();

        auto lease = TLeaseManager::CreateLease(
            Config_->InterceptedDataRetentionTime,
            BIND(&TInMemoryService::OnSessionLeaseExpired, MakeStrong(this), sessionId)
                .Via(Bootstrap_->GetStorageLightInvoker()));

        auto session = New<TInMemorySession>(inMemoryMode, Bootstrap_, std::move(lease));

        YT_LOG_DEBUG("In-memory session started (SessionId: %v)", sessionId);

        {
            // Register session.
            auto guard = WriterGuard(SessionMapLock_);
            YT_VERIFY(SessionMap_.emplace(sessionId, std::move(session)).second);
        }

        ToProto(response->mutable_session_id(), sessionId);

        context->SetResponseInfo("SessionId: %v", sessionId);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTabletNode::NProto, FinishSession)
    {
        auto sessionId = FromProto<TInMemorySessionId>(request->session_id());
        context->SetRequestInfo("SessionId: %v, TabletIds: %v, ChunkIds: %v",
            sessionId,
            MakeFormattableView(request->tablet_id(), [] (TStringBuilderBase* builder, const NYT::NProto::TGuid& tabletId) {
                FormatValue(builder, FromProto<TTabletId>(tabletId), TStringBuf());
            }),
            MakeFormattableView(request->chunk_id(), [] (TStringBuilderBase* builder, const NYT::NProto::TGuid& chunkId) {
                FormatValue(builder, FromProto<TChunkId>(chunkId), TStringBuf());
            }));

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();

        if (auto session = FindSession(sessionId)) {

            std::vector<TFuture<void>> asyncResults;
            for (int index = 0; index < request->chunk_id_size(); ++index) {
                auto tabletId = FromProto<TTabletId>(request->tablet_id(index));

                // COMPAT(ifsmirnov)
                auto tabletSnapshot = request->mount_revision_size() > 0
                    ? snapshotStore->FindTabletSnapshot(tabletId, request->mount_revision(index))
                    : snapshotStore->FindLatestTabletSnapshot(tabletId);

                if (!tabletSnapshot) {
                    YT_LOG_DEBUG("Tablet snapshot not found (TabletId: %v)", tabletId);
                    continue;
                }

                auto chunkId = FromProto<TChunkId>(request->chunk_id(index));
                auto chunkData = session->ExtractChunkData(chunkId);

                auto asyncResult = BIND(&IInMemoryManager::FinalizeChunk, Bootstrap_->GetInMemoryManager())
                    .AsyncVia(NRpc::TDispatcher::Get()->GetCompressionPoolInvoker())
                    .Run(
                        chunkId,
                        std::move(chunkData),
                        New<TRefCountedChunkMeta>(std::move(*request->mutable_chunk_meta(index))),
                        tabletSnapshot);

                asyncResults.push_back(std::move(asyncResult));
            }

            WaitFor(AllSucceeded(asyncResults))
                .ThrowOnError();

            TLeaseManager::CloseLease(session->Lease);

            {
                auto guard = WriterGuard(SessionMapLock_);
                SessionMap_.erase(sessionId);
            }

            YT_LOG_DEBUG("In-memory session finished (SessionId: %v)", sessionId);
        } else {
            YT_LOG_DEBUG("In-memory session does not exist (SessionId: %v)", sessionId);
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTabletNode::NProto, PutBlocks)
    {
        auto sessionId = FromProto<TInMemorySessionId>(request->session_id());

        context->SetRequestInfo("SessionId: %v, BlockCount: %v",
            sessionId,
            request->block_ids_size());

        if (auto session = FindSession(sessionId)) {
            RenewSessionLease(session);

            for (int index = 0; index < request->block_ids_size(); ++index) {
                auto blockId = FromProto<TBlockId>(request->block_ids(index));

                session->PutBlock(
                    blockId,
                    TBlock(request->Attachments()[index]));
            }

            bool dropped = Bootstrap_->GetMemoryUsageTracker()->IsExceeded(
                NNodeTrackerClient::EMemoryCategory::TabletStatic);

            response->set_dropped(dropped);
        } else {
            YT_LOG_DEBUG("In-memory session does not exist, blocks dropped (SessionId: %v)",
                sessionId);
            response->set_dropped(true);
        }


        context->SetResponseInfo("Dropped: %v", response->dropped());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTabletNode::NProto, PingSession)
    {
        auto sessionId = FromProto<TInMemorySessionId>(request->session_id());

        context->SetRequestInfo("SessionId: %v", sessionId);

        auto session = GetSessionOrThrow(sessionId);
        RenewSessionLease(session);

        context->Reply();
    }


    void OnSessionLeaseExpired(TInMemorySessionId sessionId)
    {
        auto guard = WriterGuard(SessionMapLock_);

        auto it = SessionMap_.find(sessionId);
        if (it == SessionMap_.end()) {
            return;
        }

        YT_LOG_INFO("Session lease expired (SessionId: %v)",
            sessionId);

        YT_VERIFY(SessionMap_.erase(sessionId) == 1);
    }

    TInMemorySessionPtr FindSession(TInMemorySessionId sessionId)
    {
        auto guard = ReaderGuard(SessionMapLock_);
        auto it = SessionMap_.find(sessionId);
        return it == SessionMap_.end() ? nullptr : it->second;
    }

    TInMemorySessionPtr GetSessionOrThrow(TInMemorySessionId sessionId)
    {
        auto session = FindSession(sessionId);
        if (!session) {
            THROW_ERROR_EXCEPTION("In-memory session %v does not exist",
                sessionId);
        }
        return session;
    }

    void RenewSessionLease(const TInMemorySessionPtr& session)
    {
        TLeaseManager::RenewLease(session->Lease);
    }
};

IServicePtr CreateInMemoryService(
    TInMemoryManagerConfigPtr config,
    NClusterNode::TBootstrap* bootstrap)
{
    return New<TInMemoryService>(config, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
