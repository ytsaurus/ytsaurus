#include "public.h"
#include "private.h"
#include "in_memory_service.h"
#include "in_memory_service_proxy.h"

#include <yt/server/node/tablet_node/in_memory_manager.h>
#include <yt/server/node/tablet_node/slot_manager.h>

#include <yt/server/node/cell_node/bootstrap.h>

#include <yt/server/lib/tablet_node/config.h>

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/dispatcher.h>

#include <yt/core/rpc/service_detail.h>

#include <yt/core/actions/signal.h>

#include <yt/core/concurrency/lease_manager.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/optional.h>

#include <yt/core/ytalloc/memory_zone.h>

namespace NYT::NTabletNode {

using namespace NRpc;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TInMemorySession)

struct TInMemorySession
    : public virtual TRefCounted
{
    TInMemorySession(IBlockCachePtr interceptingBlockCache, TLease lease)
        : InterceptingBlockCache(std::move(interceptingBlockCache))
        , Lease(std::move(lease))
    { }

    IBlockCachePtr InterceptingBlockCache;
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
        NCellNode::TBootstrap* bootstrap)
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
    NCellNode::TBootstrap* const Bootstrap_;

    TReaderWriterSpinLock SessionMapLock_;
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

        auto interceptingBlockCache = Bootstrap_->GetInMemoryManager()->CreateInterceptingBlockCache(
            inMemoryMode);

        auto session = New<TInMemorySession>(
            std::move(interceptingBlockCache),
            std::move(lease));

        YT_LOG_DEBUG("In-memory session started (SessionId: %v)", sessionId);

        RegisterSession(sessionId, std::move(session));

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

        const auto& slotManager = Bootstrap_->GetTabletSlotManager();

        for (size_t index = 0; index < request->chunk_id_size(); ++index) {
            auto tabletId = FromProto<TTabletId>(request->tablet_id(index));
            auto tabletSnapshot = slotManager->FindTabletSnapshot(tabletId);

            if (!tabletSnapshot) {
                YT_LOG_DEBUG("Tablet snapshot not found (TabletId: %v)", tabletId);
                continue;
            }

            auto asyncResult = BIND(&IInMemoryManager::FinalizeChunk, Bootstrap_->GetInMemoryManager())
                .AsyncVia(NRpc::TDispatcher::Get()->GetCompressionPoolInvoker())
                .Run(
                    FromProto<TChunkId>(request->chunk_id(index)),
                    New<TRefCountedChunkMeta>(std::move(*request->mutable_chunk_meta(index))),
                    tabletSnapshot);

            WaitFor(asyncResult)
                .ThrowOnError();
        }

        {
            TWriterGuard guard(SessionMapLock_);
            if (auto it = SessionMap_.find(sessionId)) {
                const auto& session = it->second;
                TLeaseManager::CloseLease(session->Lease);
                YT_VERIFY(SessionMap_.erase(sessionId));

                YT_LOG_DEBUG("In-memory session finished (SessionId: %v)", sessionId);
            } else {
                YT_LOG_DEBUG("In-memory session does not exist (SessionId: %v)", sessionId);
            }
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

                session->InterceptingBlockCache->Put(
                    blockId,
                    session->InterceptingBlockCache->GetSupportedBlockTypes(),
                    TBlock(request->Attachments()[index]),
                    std::nullopt);
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
        TWriterGuard guard(SessionMapLock_);

        auto it = SessionMap_.find(sessionId);
        if (it == SessionMap_.end()) {
            return;
        }

        YT_LOG_INFO("Session lease expired (SessionId: %v)",
            sessionId);

        YT_VERIFY(SessionMap_.erase(sessionId) == 1);
    }

    void RegisterSession(TInMemorySessionId sessionId, TInMemorySessionPtr session)
    {
        TWriterGuard guard(SessionMapLock_);
        YT_VERIFY(SessionMap_.emplace(sessionId, std::move(session)).second);
    }

    TInMemorySessionPtr FindSession(TInMemorySessionId sessionId)
    {
        TReaderGuard guard(SessionMapLock_);
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
    NCellNode::TBootstrap* bootstrap)
{
    return New<TInMemoryService>(config, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
