#include "in_memory_service.h"

#include "bootstrap.h"
#include "public.h"
#include "private.h"
#include "in_memory_service_proxy.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/tablet_node/in_memory_manager.h>
#include <yt/yt/server/node/tablet_node/slot_manager.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt/ytlib/misc/memory_reference_tracker.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/lease_manager.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/memory_reference_tracker.h>

namespace NYT::NTabletNode {

using namespace NRpc;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NTabletClient;

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

struct TChunkData final
{
    std::vector<NChunkClient::TBlock> Blocks;
};

using TChunkDataPtr = TIntrusivePtr<TChunkData>;

class TInterceptingBlockCache
{
public:
    TInterceptingBlockCache(EInMemoryMode mode, IBootstrap* bootstrap)
        : Mode_(mode)
        , Bootstrap_(bootstrap)
    { }

    TError PutBlock(const TBlockId& id, const TBlock& block)
    {
        auto chunkId = id.ChunkId;

        auto guard = Guard(SpinLock_);

        TChunkDataPtr data;

        auto it = ChunkIdToData_.find(chunkId);
        if (it == ChunkIdToData_.end()) {
            data = New<TChunkData>();

            auto guardOrError = TMemoryUsageTrackerGuard::TryAcquire(
                Bootstrap_
                    ->GetMemoryUsageTracker()
                    ->WithCategory(EMemoryCategory::TabletStatic),
                0 /*size*/,
                MemoryUsageGranularity);

            if (!guardOrError.IsOK()) {
                return guardOrError;
            }

            YT_LOG_INFO("Intercepted chunk data created (ChunkId: %v, Mode: %v)",
                chunkId,
                Mode_);

            // Replace the old data, if any, by a new one.
            ChunkIdToData_[chunkId] = data;
        } else {
            data = it->second;
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
        data->Blocks[id.BlockIndex].Data = TrackMemory(
            Bootstrap_->GetNodeMemoryReferenceTracker(),
            EMemoryCategory::TabletStatic,
            std::move(block.Data));

        return TError();
    }

    TChunkDataPtr ExtractChunkData(TChunkId chunkId)
    {
        auto guard = Guard(SpinLock_);
        auto it = ChunkIdToData_.find(chunkId);
        return it == ChunkIdToData_.end() ? nullptr : it->second;
    }

    EInMemoryMode GetInMemoryMode() const
    {
        return Mode_;
    }

private:
    const EInMemoryMode Mode_;
    IBootstrap* const Bootstrap_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    THashMap<TChunkId, TChunkDataPtr> ChunkIdToData_;
};

DECLARE_REFCOUNTED_STRUCT(TInMemorySession)

struct TInMemorySession
    : public TRefCounted
    , public TInterceptingBlockCache
{
    TInMemorySession(EInMemoryMode mode, IBootstrap* bootstrap, TLease lease)
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
    explicit TInMemoryService(IBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetStorageLightInvoker(),
            TInMemoryServiceProxy::GetDescriptor(),
            TabletNodeLogger,
            NullRealmId,
            bootstrap->GetNativeAuthenticator())
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PutBlocks));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishSession));
    }

private:
    IBootstrap* const Bootstrap_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SessionMapLock_);
    THashMap<TInMemorySessionId, TInMemorySessionPtr> SessionMap_;

    DECLARE_RPC_SERVICE_METHOD(NTabletNode::NProto, StartSession)
    {
        auto inMemoryMode = FromProto<EInMemoryMode>(request->in_memory_mode());

        context->SetRequestInfo("InMemoryMode: %v", inMemoryMode);

        auto sessionId = TInMemorySessionId::Create();

        const auto& inMemoryManager = Bootstrap_->GetInMemoryManager();
        auto config = inMemoryManager->GetConfig();

        auto lease = TLeaseManager::CreateLease(
            config->InterceptedDataRetentionTime,
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
            bool dropSession = false;
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

                if (!chunkData) {
                    YT_LOG_WARNING("Chunk data does not exist, dropping in-memory session (SessionId: %v, ChunkId: %v)",
                        sessionId,
                        chunkId);

                    dropSession = true;
                    break;
                }

                auto meta = New<TRefCountedChunkMeta>(std::move(*request->mutable_chunk_meta(index)));

                auto versionedChunkMeta = NTableClient::TCachedVersionedChunkMeta::Create(
                    /*prepareColumnarMeta*/ false,
                    /*memoryTracker*/ nullptr,
                    meta);

                auto asyncResult = BIND(&CreateInMemoryChunkData)
                    .AsyncVia(NRpc::TDispatcher::Get()->GetCompressionPoolInvoker())
                    .Run(
                        chunkId,
                        session->GetInMemoryMode(),
                        0,
                        chunkData->Blocks,
                        versionedChunkMeta,
                        tabletSnapshot,
                        Bootstrap_->GetNodeMemoryReferenceTracker(),
                        Bootstrap_->GetMemoryUsageTracker()->WithCategory(EMemoryCategory::TabletStatic))
                    .Apply(BIND(&IInMemoryManager::FinalizeChunk, Bootstrap_->GetInMemoryManager(), chunkId));

                asyncResults.push_back(std::move(asyncResult));
            }

            if (!dropSession) {
                WaitFor(AllSucceeded(asyncResults))
                    .ThrowOnError();
            }

            TLeaseManager::CloseLease(session->Lease);

            {
                auto guard = WriterGuard(SessionMapLock_);
                SessionMap_.erase(sessionId);
            }

            if (!dropSession) {
                YT_LOG_DEBUG("In-memory session finished (SessionId: %v)", sessionId);
            }
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

            bool dropped = false;
            for (int index = 0; index < request->block_ids_size(); ++index) {
                auto blockId = FromProto<TBlockId>(request->block_ids(index));

                auto error = session->PutBlock(
                    blockId,
                    TBlock(request->Attachments()[index]));

                if (!error.IsOK()) {
                    TLeaseManager::CloseLease(session->Lease);

                    auto guard = WriterGuard(SessionMapLock_);
                    if (SessionMap_.erase(sessionId) == 1) {
                        guard.Release();

                        YT_LOG_WARNING("In-memory session is dropped due to memory pressure (SessionId: %v, ChunkId: %v)",
                            sessionId,
                            blockId.ChunkId);
                    }

                    dropped = true;
                    break;
                }
            }

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

IServicePtr CreateInMemoryService(IBootstrap* bootstrap)
{
    return New<TInMemoryService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
