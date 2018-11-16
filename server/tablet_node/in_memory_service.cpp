#include "public.h"
#include "private.h"
#include "config.h"
#include "in_memory_service.h"
#include "in_memory_service_proxy.h"

#include <yt/server/tablet_node/in_memory_manager.h>
#include <yt/server/tablet_node/slot_manager.h>

#include <yt/server/cell_node/bootstrap.h>

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/dispatcher.h>

#include <yt/core/rpc/service_detail.h>

#include <yt/core/actions/signal.h>

#include <yt/core/concurrency/lease_manager.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/nullable.h>

namespace NYT {
namespace NTabletNode {

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
            bootstrap->GetControlInvoker(),
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

    THashMap<TInMemorySessionId, TInMemorySessionPtr> SessionMap_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    DECLARE_RPC_SERVICE_METHOD(NTabletNode::NProto, StartSession)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto inMemoryMode = FromProto<EInMemoryMode>(request->in_memory_mode());

        context->SetRequestInfo("InMemoryMode: %v", inMemoryMode);

        auto sessionId = TInMemorySessionId::Create();

        auto lease = TLeaseManager::CreateLease(
            Config_->InterceptedDataRetentionTime,
            BIND(&TInMemoryService::OnSessionLeaseExpired, MakeStrong(this), sessionId)
                .Via(Bootstrap_->GetControlInvoker()));

        auto interceptingBlockCache = Bootstrap_->GetInMemoryManager()->CreateInterceptingBlockCache(
            inMemoryMode);

        auto session = New<TInMemorySession>(
            std::move(interceptingBlockCache),
            std::move(lease));

        LOG_DEBUG("In-memory session started (SessionId: %v)", sessionId);

        YCHECK(SessionMap_.emplace(sessionId, session).second);

        ToProto(response->mutable_session_id(), sessionId);

        context->SetResponseInfo("SessionId: %v", sessionId);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTabletNode::NProto, FinishSession)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto sessionId = FromProto<TInMemorySessionId>(request->session_id());
        context->SetRequestInfo("SessionId: %v, TabletIds: %v, ChunkIds: %v",
            sessionId,
            MakeFormattableRange(request->tablet_id(), [] (TStringBuilder* builder, const NYT::NProto::TGuid& tabletId) {
                FormatValue(builder, FromProto<TTabletId>(tabletId), TStringBuf());
            }),
            MakeFormattableRange(request->chunk_id(), [] (TStringBuilder* builder, const NYT::NProto::TGuid& chunkId) {
                FormatValue(builder, FromProto<TChunkId>(chunkId), TStringBuf());
            }));

        const auto& slotManager = Bootstrap_->GetTabletSlotManager();

        for (size_t index = 0; index < request->chunk_id_size(); ++index) {
            auto tabletId = FromProto<TTabletId>(request->tablet_id(index));
            auto tabletSnapshot = slotManager->FindTabletSnapshot(tabletId);

            if (!tabletSnapshot) {
                LOG_DEBUG("Tablet snapshot not found (TabletId: %v)", tabletId);
                continue;
            }

            auto asyncResult = BIND(&IInMemoryManager::FinalizeChunk, Bootstrap_->GetInMemoryManager())
                .AsyncVia(NChunkClient::TDispatcher::Get()->GetCompressionPoolInvoker())
                .Run(
                    FromProto<TChunkId>(request->chunk_id(index)),
                    New<TRefCountedChunkMeta>(std::move(*request->mutable_chunk_meta(index))),
                    tabletSnapshot);

            WaitFor(asyncResult)
                .ThrowOnError();
        }

        if (auto session = FindSession(sessionId)) {
            TLeaseManager::CloseLease(session->Lease);
            YCHECK(SessionMap_.erase(sessionId));

            LOG_DEBUG("In-memory session finished (SessionId: %v)", sessionId);
        } else {
            LOG_DEBUG("In-memory session does not exist (SessionId: %v)", sessionId);
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTabletNode::NProto, PutBlocks)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto sessionId = FromProto<TInMemorySessionId>(request->session_id());
        context->SetRequestInfo("SessionId: %v", sessionId);

        auto session = FindSession(sessionId);

        if (!session) {
            LOG_DEBUG("In-memory session does not exist (SessionId: %v)", sessionId);

            response->set_dropped(true);

            context->SetResponseInfo("Dropped: %v", true);
            context->Reply();

            return;
        }

        TLeaseManager::RenewLease(session->Lease);

        for (size_t index = 0; index < request->block_ids_size(); ++index) {
            auto blockId = FromProto<TBlockId>(request->block_ids(index));

            session->InterceptingBlockCache->Put(
                blockId,
                session->InterceptingBlockCache->GetSupportedBlockTypes(),
                TBlock(request->Attachments()[index]),
                Null);
        }

        bool dropped = Bootstrap_->GetMemoryUsageTracker()->IsExceeded(
            NNodeTrackerClient::EMemoryCategory::TabletStatic);

        response->set_dropped(dropped);

        context->SetResponseInfo("Dropped: %v", dropped);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTabletNode::NProto, PingSession)
    {
        auto sessionId = FromProto<TInMemorySessionId>(request->session_id());
        context->SetRequestInfo("SessionId: %v", sessionId);

        if (auto session = FindSession(sessionId)) {
            TLeaseManager::RenewLease(session->Lease);
        } else {
            LOG_DEBUG("In-memory session does not exist (SessionId: %v)", sessionId);
        }

        context->Reply();
    }

    void OnSessionLeaseExpired(TInMemorySessionId sessionId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto session = FindSession(sessionId);
        if (!session) {
            return;
        }

        LOG_INFO("Session lease expired (SessionId: %v)",
            sessionId);

        YCHECK(SessionMap_.erase(sessionId));
    }

    TInMemorySessionPtr FindSession(TInMemorySessionId sessionId)
    {
        auto it = SessionMap_.find(sessionId);
        return it == SessionMap_.end() ? nullptr : it->second;
    }
};

IServicePtr CreateInMemoryService(
    TInMemoryManagerConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
{
    return New<TInMemoryService>(config, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
