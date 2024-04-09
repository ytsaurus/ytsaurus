#include "hive_manager.h"

#include "avenue_directory.h"
#include "config.h"
#include "helpers.h"
#include "logical_time_registry.h"
#include "mailbox.h"
#include "private.h"

#include <yt/yt/server/lib/election/election_manager.h>

#include <yt/yt/server/lib/hydra/composite_automaton.h>
#include <yt/yt/server/lib/hydra/hydra_manager.h>
#include <yt/yt/server/lib/hydra/hydra_service.h>
#include <yt/yt/server/lib/hydra/mutation.h>
#include <yt/yt/server/lib/hydra/mutation_context.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/hive_service_proxy.h>

#include <yt/yt/ytlib/hydra/config.h>
#include <yt/yt/ytlib/hydra/peer_channel.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/fls.h>
#include <yt/yt/core/concurrency/async_batcher.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>
#include <yt/yt/core/rpc/dispatcher.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NHiveServer {

using namespace NNet;
using namespace NRpc;
using namespace NRpc::NProto;
using namespace NHydra;
using namespace NHydra::NProto;
using namespace NHiveClient;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NTracing;
using namespace NProfiling;
using namespace NObjectClient;

using NYT::ToProto;
using NYT::FromProto;

using NHiveClient::NProto::TEncapsulatedMessage;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto ReadOnlyCheckPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

static NConcurrency::TFlsSlot<TCellId> HiveMutationSenderId;

bool IsHiveMutation()
{
    return static_cast<bool>(*HiveMutationSenderId);
}

TCellId GetHiveMutationSenderId()
{
    return *HiveMutationSenderId;
}

class THiveMutationGuard
    : private TNonCopyable
{
public:
    THiveMutationGuard(TCellId senderId)
    {
        YT_ASSERT(!*HiveMutationSenderId);
        *HiveMutationSenderId = senderId;
    }

    ~THiveMutationGuard()
    {
        *HiveMutationSenderId = {};
    }
};

////////////////////////////////////////////////////////////////////////////////

class THiveManager
    : public IHiveManager
    , public THydraServiceBase
    , public TCompositeAutomatonPart
{
public:
    THiveManager(
        THiveManagerConfigPtr config,
        ICellDirectoryPtr cellDirectory,
        IAvenueDirectoryPtr avenueDirectory,
        TCellId selfCellId,
        IInvokerPtr automatonInvoker,
        IHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr automaton,
        IUpstreamSynchronizerPtr upstreamSynchronizer,
        IAuthenticatorPtr authenticator)
        : THydraServiceBase(
            hydraManager,
            hydraManager->CreateGuardedAutomatonInvoker(automatonInvoker),
            THiveServiceProxy::GetDescriptor(),
            HiveServerLogger,
            selfCellId,
            std::move(upstreamSynchronizer),
            std::move(authenticator))
        , TCompositeAutomatonPart(
            hydraManager,
            automaton,
            automatonInvoker)
        , SelfCellId_(selfCellId)
        , Config_(std::move(config))
        , CellDirectory_(std::move(cellDirectory))
        , AvenueDirectory_(std::move(avenueDirectory))
        , AutomatonInvoker_(std::move(automatonInvoker))
        , GuardedAutomatonInvoker_(hydraManager->CreateGuardedAutomatonInvoker(AutomatonInvoker_))
        , HydraManager_(std::move(hydraManager))
        , Profiler_(HiveServerProfiler
            .WithGlobal()
            .WithSparse()
            .WithTag("cell_id", ToString(selfCellId)))
        , LogicalTimeRegistry_(New<TLogicalTimeRegistry>(
            Config_->LogicalTimeRegistry,
            AutomatonInvoker_,
            HydraManager_,
            Profiler_))
    {
        SyncPostingTimeCounter_ = Profiler_.TimeCounter("/sync_posting_time");
        AsyncPostingTimeCounter_ = Profiler_.TimeCounter("/async_posting_time");

        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(Ping)
            .SetInvoker(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(SyncCells)
            .SetHeavy(true));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(PostMessages)
            .SetHeavy(true));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(SendMessages)
            .SetHeavy(true));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(SyncWithOthers)
            .SetHeavy(true));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(GetConsistentState));

        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&THiveManager::HydraAcknowledgeMessages, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&THiveManager::HydraPostMessages, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&THiveManager::HydraSendMessages, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&THiveManager::HydraRegisterMailbox, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&THiveManager::HydraUnregisterMailbox, Unretained(this)));

        RegisterLoader(
            "HiveManager.Keys",
            BIND(&THiveManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "HiveManager.Values",
            BIND(&THiveManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "HiveManager.Keys",
            BIND(&THiveManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "HiveManager.Values",
            BIND(&THiveManager::SaveValues, Unretained(this)));

        OrchidService_ = CreateOrchidService();
        LamportClock_ = LogicalTimeRegistry_->GetClock();

        if (AvenueDirectory_) {
            AvenueDirectory_->SubscribeEndpointUpdated(
                BIND_NO_PROPAGATE(&THiveManager::OnAvenueDirectoryEndpointUpdated, MakeWeak(this))
                    .Via(AutomatonInvoker_));
        }
    }

    IServicePtr GetRpcService() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return this;
    }

    IYPathServicePtr GetOrchidService() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return OrchidService_;
    }

    TCellId GetSelfCellId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return SelfCellId_;
    }

    TCellMailbox* CreateCellMailbox(TCellId cellId, bool allowResurrection = false) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(HasHydraContext());

        if (RemovedCellIds_.erase(cellId) != 0 && !allowResurrection) {
            YT_LOG_ALERT("Mailbox has been resurrected (SelfCellId: %v, CellId: %v)",
                SelfCellId_,
                cellId);
        }

        auto mailboxHolder = std::make_unique<TCellMailbox>(cellId);
        auto* mailbox = CellMailboxMap_.Insert(cellId, std::move(mailboxHolder));

        {
            auto guard = WriterGuard(MailboxRuntimeDataMapLock_);
            YT_VERIFY(MailboxRuntimeDataMap_.emplace(cellId, mailbox->GetRuntimeData()).second);
        }

        if (!IsRecovery()) {
            SendPeriodicPing(mailbox);
        }

        YT_LOG_INFO("Mailbox created (SelfCellId: %v, CellId: %v)",
            SelfCellId_,
            mailbox->GetCellId());
        return mailbox;
    }

    TMailbox* FindMailbox(TEndpointId endpointId) const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (IsAvenueEndpointType(TypeFromId(endpointId))) {
            return AvenueMailboxMap_.Find(endpointId);
        } else {
            return CellMailboxMap_.Find(endpointId);
        }
    }

    TMailbox* GetMailbox(TEndpointId endpointId) const override
    {
        auto* mailbox = FindMailbox(endpointId);
        YT_VERIFY(mailbox);
        return mailbox;
    }

    TMailboxRuntimeDataPtr FindMailboxRuntimeData(TCellId cellId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(MailboxRuntimeDataMapLock_);
        auto it = MailboxRuntimeDataMap_.find(cellId);
        return it == MailboxRuntimeDataMap_.end()
            ? nullptr
            : it->second;
    }

    TCellMailbox* GetOrCreateCellMailbox(TCellId cellId) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(HasHydraContext());

        auto* mailbox = CellMailboxMap_.Find(cellId);
        if (!mailbox) {
            mailbox = CreateCellMailbox(cellId);
        }
        return mailbox;
    }

    TMailbox* GetMailboxOrThrow(TEndpointId endpointId) const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* mailbox = FindMailbox(endpointId);
        if (!mailbox) {
            THROW_ERROR_EXCEPTION("No such mailbox %v",
                endpointId);
        }
        return mailbox;
    }

    void RemoveCellMailbox(TCellMailbox* mailbox) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto cellId = mailbox->GetCellId();

        // Following updates will change the map so we make a copy.
        auto registeredAvenues = mailbox->RegisteredAvenues();
        for (auto [id, avenueMailbox] : registeredAvenues) {
            UpdateAvenueCellConnection(avenueMailbox, /*cellMailbox*/ nullptr);
        }

        CellMailboxMap_.Remove(cellId);

        {
            auto guard = WriterGuard(MailboxRuntimeDataMapLock_);
            EraseOrCrash(MailboxRuntimeDataMap_, cellId);
        }

        if (!RemovedCellIds_.insert(cellId).second) {
            YT_LOG_ALERT("Mailbox is already removed (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                cellId);
        }

        YT_LOG_INFO("Mailbox removed (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            cellId);
    }

    void RegisterAvenueEndpoint(
        TAvenueEndpointId selfEndpointId,
        TPersistentMailboxState&& cookie) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto otherEndpointId = GetSiblingAvenueEndpointId(selfEndpointId);

        if (AvenueMailboxMap_.Find(otherEndpointId)) {
            YT_LOG_ALERT("Attempted to register already existing avenue endpoint, ignored "
                "(SelfCellId: %v, SelfEndpointId: %v)",
                SelfCellId_,
                selfEndpointId);
            return;
        }

        auto holder = std::make_unique<TAvenueMailbox>(otherEndpointId);
        auto* mailbox = AvenueMailboxMap_.Insert(otherEndpointId, std::move(holder));

        *static_cast<TPersistentMailboxState*>(mailbox) = std::move(cookie);

        {
            auto guard = WriterGuard(MailboxRuntimeDataMapLock_);
            EmplaceOrCrash(MailboxRuntimeDataMap_, otherEndpointId, mailbox->GetRuntimeData());
        }

        mailbox->UpdateLastOutcomingMessageId();
        mailbox->SetNextTransientIncomingMessageId(mailbox->GetNextPersistentIncomingMessageId());

        UpdateAvenueCellConnection(
            mailbox,
            AvenueDirectory_->FindCellIdByEndpointId(mailbox->GetEndpointId()));

        YT_LOG_DEBUG("Avenue mailbox registered "
            "(SelfCellId: %v, SelfEndpointId: %v, OtherEndpointId: %v, "
            "FirstOutcomingMessageId: %v, OutcomingMessageCount: %v, "
            "NextPersistentIncomingMessageId: %v)",
            SelfCellId_,
            selfEndpointId,
            otherEndpointId,
            mailbox->GetFirstOutcomingMessageId(),
            mailbox->OutcomingMessages().size(),
            mailbox->GetNextPersistentIncomingMessageId());
    }

    TPersistentMailboxState UnregisterAvenueEndpoint(TAvenueEndpointId selfEndpointId) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto otherEndpointId = GetSiblingAvenueEndpointId(selfEndpointId);

        auto* mailbox = AvenueMailboxMap_.Find(otherEndpointId);
        if (!mailbox) {
            YT_LOG_ALERT("Attempted to remove a non-existing avenue endpoint, ignored "
                "(SelfCellId: %v, SelfEndpointId: %v)",
                SelfCellId_,
                selfEndpointId);
            return {};
        }

        {
            auto guard = WriterGuard(MailboxRuntimeDataMapLock_);
            EraseOrCrash(MailboxRuntimeDataMap_, otherEndpointId);
        }

        UpdateAvenueCellConnection(mailbox, /*cellMailbox*/ nullptr);

        auto cookie = std::move(*static_cast<TPersistentMailboxState*>(mailbox));

        AvenueMailboxMap_.Remove(otherEndpointId);

        YT_LOG_DEBUG("Avenue mailbox unregistered "
            "(SelfCellId: %v, SelfEndpointId: %v, OtherEndpointId: %v, "
            "FirstOutcomingMessageId: %v, OutcomingMessageCount: %v, "
            "NextPersistentIncomingMessageId: %v)",
            SelfCellId_,
            selfEndpointId,
            otherEndpointId,
            cookie.GetFirstOutcomingMessageId(),
            cookie.OutcomingMessages().size(),
            cookie.GetNextPersistentIncomingMessageId());

        return cookie;
    }

    void PostMessage(TMailbox* mailbox, const TSerializedMessagePtr& message, bool reliable) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        PostMessage(TMailboxList{mailbox}, message, reliable);
    }

    void PostMessage(const TMailboxList& mailboxes, const TSerializedMessagePtr& message, bool reliable) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (reliable) {
            ReliablePostMessage(mailboxes, message);
        } else {
            UnreliablePostMessage(mailboxes, message);
        }
    }

    void PostMessage(TMailbox* mailbox, const ::google::protobuf::MessageLite& message, bool reliable) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        PostMessage(mailbox, SerializeOutcomingMessage(message), reliable);
    }

    void PostMessage(const TMailboxList& mailboxes, const ::google::protobuf::MessageLite& message, bool reliable) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        PostMessage(mailboxes, SerializeOutcomingMessage(message), reliable);
    }

    TFuture<void> SyncWith(TCellId cellId, bool enableBatching) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (cellId == SelfCellId_) {
            return VoidFuture;
        }

        if (enableBatching) {
            auto batcher = GetOrCreateSyncBatcher(cellId);
            if (batcher) {
                return batcher->Run();
            } else {
                return MakeFuture(TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped"));
            }
        } else {
            return DoSyncWithCore(cellId).ToImmediatelyCancelable();
        }
    }

    DECLARE_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS_OVERRIDE(CellMailbox, CellMailboxes, TCellMailbox);
    DECLARE_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS_OVERRIDE(AvenueMailbox, AvenueMailboxes, TAvenueMailbox);

private:
    const TCellId SelfCellId_;
    const THiveManagerConfigPtr Config_;
    const ICellDirectoryPtr CellDirectory_;
    const IAvenueDirectoryPtr AvenueDirectory_;
    const IInvokerPtr AutomatonInvoker_;
    const IInvokerPtr GuardedAutomatonInvoker_;
    const IHydraManagerPtr HydraManager_;
    const TProfiler Profiler_;

    IYPathServicePtr OrchidService_;

    TLogicalTimeRegistryPtr LogicalTimeRegistry_;
    TLogicalTimeRegistry::TLamportClock* LamportClock_;

    TEntityMap<TCellMailbox> CellMailboxMap_;
    TEntityMap<TAvenueMailbox> AvenueMailboxMap_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, MailboxRuntimeDataMapLock_);
    THashMap<TCellId, TMailboxRuntimeDataPtr> MailboxRuntimeDataMap_;

    THashSet<TCellId> RemovedCellIds_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, CellToIdToSyncBatcherLock_);
    THashMap<TCellId, TIntrusivePtr<TAsyncBatcher<void>>> CellToIdToSyncBatcher_;
    bool SyncBatchersInitialized_ = false;

    TPeriodicExecutorPtr ReadOnlyCheckExecutor_;

    TTimeCounter SyncPostingTimeCounter_;
    TTimeCounter AsyncPostingTimeCounter_;
    THashMap<TCellId, TTimeCounter> PerCellSyncTimeCounter_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    // RPC handlers.

    DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto, Ping)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto srcCellId = FromProto<TCellId>(request->src_cell_id());

        context->SetRequestInfo("SrcCellId: %v, DstCellId: %v",
            srcCellId,
            SelfCellId_);

        HydraManager_->ValidatePeer(EPeerKind::Leader);

        auto runtimeData = FindMailboxRuntimeData(srcCellId);
        auto lastOutcomingMessageId = runtimeData
            ? std::make_optional(runtimeData->LastOutcomingMessageId.load())
            : std::nullopt;

        if (lastOutcomingMessageId) {
            response->set_last_outcoming_message_id(*lastOutcomingMessageId);
        }

        context->SetResponseInfo("NextTransientIncomingMessageId: %v",
            lastOutcomingMessageId);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto, SyncCells)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        context->SetRequestInfo();

        ValidatePeer(EPeerKind::LeaderOrFollower);
        SyncWithUpstream();

        context->ReplyFrom(
            BIND([=, this, this_ = MakeStrong(this)] {
                auto knownCells = FromProto<std::vector<TCellInfo>>(request->known_cells());
                auto syncResult = CellDirectory_->Synchronize(knownCells);

                for (const auto& request : syncResult.ReconfigureRequests) {
                    YT_LOG_DEBUG("Requesting cell reconfiguration (CellId: %v, ConfigVersion: %v -> %v)",
                        request.NewDescriptor->CellId,
                        request.OldConfigVersion,
                        request.NewDescriptor->ConfigVersion);
                    auto* protoInfo = response->add_cells_to_reconfigure();
                    ToProto(protoInfo->mutable_cell_descriptor(), *request.NewDescriptor);
                }

                for (const auto& request : syncResult.UnregisterRequests) {
                    YT_LOG_DEBUG("Requesting cell unregistration (CellId: %v)",
                        request.CellId);
                    auto* unregisterInfo = response->add_cells_to_unregister();
                    ToProto(unregisterInfo->mutable_cell_id(), request.CellId);
                }
            })
                .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())
                .Run());
    }

    bool DoPostMessages(
        const NHiveClient::NProto::TReqPostMessages* request,
        NHiveClient::NProto::TRspPostMessages* response)
    {
        auto srcEndpointId = FromProto<TCellId>(request->src_endpoint_id());
        auto firstMessageId = request->first_message_id();
        int messageCount = request->messages_size();

        auto* mailbox = FindMailbox(srcEndpointId);
        if (!mailbox) {
            YT_LOG_DEBUG("Received a message to a non-registered avenue mailbox "
                "(SrcEndpointId: %v, DstEndpointId: %v)",
                srcEndpointId,
                GetSiblingAvenueEndpointId(srcEndpointId));
            response->set_next_transient_incoming_message_id(-1);
            return false;
        }

        auto nextTransientIncomingMessageId = mailbox->GetNextTransientIncomingMessageId();
        YT_VERIFY(nextTransientIncomingMessageId >= 0);

        bool shouldCommit = nextTransientIncomingMessageId == firstMessageId && messageCount > 0;
        if (shouldCommit) {
            YT_LOG_DEBUG("Committing reliable incoming messages (%v, "
                "MessageIds: %v-%v)",
                FormatIncomingMailboxEndpoints(mailbox),
                firstMessageId,
                firstMessageId + messageCount - 1);

            mailbox->SetNextTransientIncomingMessageId(nextTransientIncomingMessageId + messageCount);
        }

        response->set_next_transient_incoming_message_id(nextTransientIncomingMessageId);
        response->set_next_persistent_incoming_message_id(mailbox->GetNextPersistentIncomingMessageId());

        return shouldCommit;
    }

    DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto, PostMessages)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto srcCellId = FromProto<TCellId>(request->src_endpoint_id());
        auto firstMessageId = request->first_message_id();
        int messageCount = request->messages_size();

        context->SetRequestInfo("SrcCellId: %v, DstCellId: %v, MessageIds: %v-%v",
            srcCellId,
            SelfCellId_,
            firstMessageId,
            firstMessageId + messageCount - 1);

        ValidatePeer(EPeerKind::Leader);

        ValidateCellNotRemoved(srcCellId);

        auto* mailbox = FindMailbox(srcCellId);
        if (!mailbox) {
            NHiveServer::NProto::TReqRegisterMailbox hydraRequest;
            ToProto(hydraRequest.mutable_cell_id(), srcCellId);
            YT_UNUSED_FUTURE(CreateMutation(HydraManager_, hydraRequest)
                ->CommitAndLog(Logger));

            THROW_ERROR_EXCEPTION(
                NHiveClient::EErrorCode::MailboxNotCreatedYet,
                "Mailbox %v is not created yet",
                srcCellId);
        }

        bool shouldCommit = DoPostMessages(request, response);
        if (!shouldCommit) {
            request->clear_messages();
        }

        for (auto& subrequest : *request->mutable_avenue_subrequests()) {
            if (DoPostMessages(&subrequest, response->add_avenue_subresponses())) {
                shouldCommit = true;
            } else {
                subrequest.clear_messages();
            }
        }

        if (shouldCommit) {
            YT_UNUSED_FUTURE(CreatePostMessagesMutation(*request)
                ->CommitAndLog(Logger));
        }

        auto nextTransientIncomingMessageId = mailbox->GetNextTransientIncomingMessageId();
        auto nextPersistentIncomingMessageId = mailbox->GetNextPersistentIncomingMessageId();
        context->SetResponseInfo("NextPersistentIncomingMessageId: %v, NextTransientIncomingMessageId: %v",
            nextPersistentIncomingMessageId,
            nextTransientIncomingMessageId);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto, SendMessages)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto srcCellId = FromProto<TCellId>(request->src_cell_id());
        int messageCount = request->messages_size();

        context->SetRequestInfo("SrcCellId: %v, DstCellId: %v, MessageCount: %v",
            srcCellId,
            SelfCellId_,
            messageCount);

        ValidatePeer(EPeerKind::Leader);

        YT_LOG_DEBUG("Committing unreliable incoming messages (SrcCellId: %v, DstCellId: %v, "
            "MessageCount: %v)",
            srcCellId,
            SelfCellId_,
            messageCount);

        auto mutation = CreateSendMessagesMutation(context);
        mutation->SetCurrentTraceContext();
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto, SyncWithOthers)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto srcCellIds = FromProto<std::vector<TCellId>>(request->src_cell_ids());

        context->SetRequestInfo("SrcCellIds: %v",
            srcCellIds);

        ValidatePeer(EPeerKind::Leader);

        std::vector<TFuture<void>> asyncResults;
        for (auto cellId : srcCellIds) {
            asyncResults.push_back(SyncWith(cellId, true));
        }

        context->ReplyFrom(AllSucceeded(asyncResults));
    }

    DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto, GetConsistentState)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto requestTime = request->has_logical_time()
            ? std::make_optional<TLogicalTime>(request->logical_time())
            : std::nullopt;

        context->SetRequestInfo("LogicalTime: %v",
            requestTime);

        auto [logicalTime, sequenceNumber] = LogicalTimeRegistry_->GetConsistentState(requestTime);
        response->set_logical_time(logicalTime.Underlying());
        response->set_sequence_number(sequenceNumber);

        context->SetResponseInfo("StateLogicalTime: %v, StateSequenceNumber: %v",
            logicalTime,
            sequenceNumber);
        context->Reply();
    }

    // Hydra handlers.

    void HydraAcknowledgeMessages(NHiveServer::NProto::TReqAcknowledgeMessages* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto cellId = FromProto<TCellId>(request->cell_id());
        auto* mailbox = FindMailbox(cellId);
        if (!mailbox) {
            return;
        }

        mailbox->SetAcknowledgeInProgress(false);

        auto nextPersistentIncomingMessageId = request->next_persistent_incoming_message_id();
        auto acknowledgeCount = nextPersistentIncomingMessageId - mailbox->GetFirstOutcomingMessageId();
        if (acknowledgeCount <= 0) {
            YT_LOG_DEBUG("No messages acknowledged (%v, "
                "NextPersistentIncomingMessageId: %v, FirstOutcomingMessageId: %v)",
                FormatOutgoingMailboxEndpoints(mailbox),
                nextPersistentIncomingMessageId,
                mailbox->GetFirstOutcomingMessageId());
            return;
        }

        auto& outcomingMessages = mailbox->OutcomingMessages();
        if (acknowledgeCount > std::ssize(outcomingMessages)) {
            YT_LOG_ALERT("Requested to acknowledge too many messages (%v, "
                "NextPersistentIncomingMessageId: %v, FirstOutcomingMessageId: %v, OutcomingMessageCount: %v)",
                FormatOutgoingMailboxEndpoints(mailbox),
                nextPersistentIncomingMessageId,
                mailbox->GetFirstOutcomingMessageId(),
                outcomingMessages.size());
            return;
        }

        outcomingMessages.erase(outcomingMessages.begin(), outcomingMessages.begin() + acknowledgeCount);
        mailbox->SetFirstOutcomingMessageId(mailbox->GetFirstOutcomingMessageId() + acknowledgeCount);
        mailbox->UpdateLastOutcomingMessageId();

        YT_LOG_DEBUG("Messages acknowledged (%v, "
            "FirstOutcomingMessageId: %v)",
            FormatOutgoingMailboxEndpoints(mailbox),
            mailbox->GetFirstOutcomingMessageId());

        if (mailbox->IsAvenue()) {
            auto* avenueMailbox = mailbox->AsAvenue();
            auto* cellMailbox = avenueMailbox->GetCellMailbox();

            if (!avenueMailbox->IsActive() && cellMailbox) {
                if (auto* cellMailbox = avenueMailbox->GetCellMailbox()) {
                    cellMailbox->ActiveAvenues().erase(avenueMailbox->GetEndpointId());
                    YT_LOG_DEBUG(
                        "Avenue became inactive at cell (AvenueEndpointId: %v, CellId: %v)",
                        avenueMailbox->GetEndpointId(),
                        cellMailbox->GetCellId());
                }
            }
        }
    }

    void HydraPostMessages(NHiveClient::NProto::TReqPostMessages* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto srcCellId = FromProto<TCellId>(request->src_endpoint_id());

        ValidateCellNotRemoved(srcCellId);

        auto firstMessageId = request->first_message_id();
        auto* mailbox = FindMailbox(srcCellId);
        if (!mailbox) {
            if (firstMessageId != 0) {
                YT_LOG_ALERT("Received a non-initial message to a missing mailbox (SrcCellId: %v, MessageId: %v)",
                    srcCellId,
                    firstMessageId);
                return;
            }
            mailbox = CreateCellMailbox(srcCellId);
        }

        ApplyReliableIncomingMessages(mailbox, request);

        for (const auto& subrequest : request->avenue_subrequests()) {
            auto srcId = FromProto<TAvenueEndpointId>(subrequest.src_endpoint_id());
            auto* mailbox = FindMailbox(srcId);
            if (!mailbox) {
                YT_LOG_INFO(
                    "Received a message to a missing avenue mailbox (SrcId: %v, MessageId: %v)",
                    srcId,
                    firstMessageId);
                continue;
            }
            ApplyReliableIncomingMessages(mailbox, &subrequest);
        }
    }

    void HydraSendMessages(
        const TCtxSendMessagesPtr& /*context*/,
        NHiveClient::NProto::TReqSendMessages* request,
        NHiveClient::NProto::TRspSendMessages* /*response*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto srcCellId = FromProto<TCellId>(request->src_cell_id());
        auto* mailbox = GetMailboxOrThrow(srcCellId)->AsCell();
        ApplyUnreliableIncomingMessages(mailbox, request);
    }

    void HydraRegisterMailbox(NHiveServer::NProto::TReqRegisterMailbox* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto cellId = FromProto<TCellId>(request->cell_id());
        if (RemovedCellIds_.contains(cellId)) {
            YT_LOG_INFO("Mailbox is already removed (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                cellId);
            return;
        }

        GetOrCreateCellMailbox(cellId);
    }

    void HydraUnregisterMailbox(NHiveServer::NProto::TReqUnregisterMailbox* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto cellId = FromProto<TCellId>(request->cell_id());
        if (auto* mailbox = FindCellMailbox(cellId)) {
            RemoveCellMailbox(mailbox);
        }
    }


    NRpc::IChannelPtr FindMailboxChannel(TCellMailbox* mailbox)
    {
        auto now = GetCpuInstant();
        auto cachedChannel = mailbox->GetCachedChannel();
        if (cachedChannel && now < mailbox->GetCachedChannelDeadline()) {
            return cachedChannel;
        }

        auto channel = CellDirectory_->FindChannelByCellId(mailbox->GetCellId());
        if (!channel) {
            return nullptr;
        }

        mailbox->SetCachedChannel(channel);
        mailbox->SetCachedChannelDeadline(now + DurationToCpuDuration(Config_->CachedChannelTimeout));

        return channel;
    }

    void ReliablePostMessage(const TMailboxList& mailboxes, const TSerializedMessagePtr& message)
    {
        // A typical mistake is posting a reliable Hive message outside of a mutation.
        YT_VERIFY(HasHydraContext());

        TStringBuilder logMessageBuilder;
        logMessageBuilder.AppendFormat("Reliable outcoming message added (MutationType: %v, SrcCellId: %v, DstIds: {",
            message->Type,
            SelfCellId_);

        auto* traceContext = NTracing::TryGetCurrentTraceContext();

        auto* mutationContext = TryGetCurrentMutationContext();

        auto logicalTime = LamportClock_->GetTime();
        if (mutationContext) {
            logicalTime = LamportClock_->Tick();
            mutationContext->CombineStateHash(message->Type, message->Data);
        }

        for (auto* mailbox : mailboxes) {
            auto messageId =
                mailbox->GetFirstOutcomingMessageId() +
                mailbox->OutcomingMessages().size();

            if (mutationContext) {
                mutationContext->CombineStateHash(messageId, mailbox->GetEndpointId());
            }

            mailbox->OutcomingMessages().push_back(TPersistentMailboxState::TOutcomingMessage{
                .SerializedMessage = message,
                .TraceContext = traceContext,
                .Time = logicalTime
            });
            mailbox->UpdateLastOutcomingMessageId();

            if (mailbox != mailboxes.front()) {
                logMessageBuilder.AppendString(TStringBuf(", "));
            }
            logMessageBuilder.AppendFormat("%v=>%v",
                mailbox->GetEndpointId(),
                messageId);

            if (mailbox->IsAvenue()) {
                if (auto* cellMailbox = mailbox->AsAvenue()->GetCellMailbox()) {
                    if (cellMailbox->ActiveAvenues().emplace(
                        mailbox->GetEndpointId(),
                        mailbox->AsAvenue()).second)
                    {
                        YT_LOG_DEBUG(
                            "Avenue became active at cell (AvenueEndpointId: %v, CellId: %v)",
                            mailbox->GetEndpointId(),
                            cellMailbox->GetCellId());
                    }
                }
            }

            SchedulePostOutcomingMessages(mailbox);
        }

        if (mutationContext) {
            logMessageBuilder.AppendFormat("}, LogicalTime: %v, SequenceNumber: %v)",
                logicalTime,
                mutationContext->GetSequenceNumber());
        }
        YT_LOG_DEBUG(logMessageBuilder.Flush());
    }

    void UnreliablePostMessage(const TMailboxList& mailboxes, const TSerializedMessagePtr& message)
    {
        TWallTimer timer;
        auto finally = Finally([&] {
            SyncPostingTimeCounter_.Add(timer.GetElapsedTime());
        });

        TStringBuilder logMessageBuilder;
        logMessageBuilder.AppendFormat("Sending unreliable outcoming message (MutationType: %v, SrcCellId: %v, DstCellIds: [",
            message->Type,
            SelfCellId_);

        auto* traceContext = NTracing::TryGetCurrentTraceContext();

        for (auto* mailboxBase : mailboxes) {
            if (!mailboxBase->IsCell()) {
                THROW_ERROR_EXCEPTION("Cannot post unreliable messages to a non-cell mailbox %v",
                    mailboxBase->GetEndpointId());
            }
            auto* mailbox = mailboxBase->AsCell();

            if (!mailbox->GetConnected()) {
                continue;
            }

            auto channel = FindMailboxChannel(mailbox);
            if (!channel) {
                continue;
            }

            if (mailbox != mailboxes.front()) {
                logMessageBuilder.AppendString(TStringBuf(", "));
            }
            logMessageBuilder.AppendFormat("%v", mailbox->GetCellId());

            THiveServiceProxy proxy(std::move(channel));

            auto req = proxy.SendMessages();
            req->SetTimeout(Config_->SendRpcTimeout);
            ToProto(req->mutable_src_cell_id(), SelfCellId_);
            auto* protoMessage = req->add_messages();
            protoMessage->set_type(message->Type);
            protoMessage->set_data(message->Data);
            if (traceContext) {
                ToProto(protoMessage->mutable_tracing_ext(), traceContext);
            }

            req->Invoke().Subscribe(
                BIND(&THiveManager::OnSendMessagesResponse, MakeStrong(this), mailbox->GetCellId())
                    .Via(EpochAutomatonInvoker_));
        }

        logMessageBuilder.AppendString(TStringBuf("])"));
        YT_LOG_DEBUG(logMessageBuilder.Flush());
    }


    void SetMailboxConnected(TCellMailbox* mailbox)
    {
        if (mailbox->GetConnected()) {
            return;
        }

        mailbox->SetConnected(true);
        YT_VERIFY(mailbox->SyncRequests().empty());

        auto doSetConnected = [] (TMailbox* mailbox) {
            mailbox->SetFirstInFlightOutcomingMessageId(mailbox->GetFirstOutcomingMessageId());
            YT_VERIFY(mailbox->GetInFlightOutcomingMessageCount() == 0);
        };

        doSetConnected(mailbox);
        for (const auto& [avenueId, avenueMailbox] : mailbox->RegisteredAvenues()) {
            doSetConnected(avenueMailbox);
        }

        YT_LOG_INFO("Mailbox connected (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            mailbox->GetCellId());

        PostOutcomingMessages(mailbox, true);
    }

    void MaybeDisconnectMailboxOnError(TCellMailbox* mailbox, const TError& error)
    {
        if (error.FindMatching(NHydra::EErrorCode::ReadOnly)) {
            return;
        }

        SetMailboxDisconnected(mailbox);
    }

    void SetMailboxDisconnected(TMailbox* mailboxBase)
    {
        if (!mailboxBase->IsCell()) {
            return;
        }

        auto* mailbox = mailboxBase->AsCell();
        if (!mailbox->GetConnected()) {
            return;
        }

        {
            NTracing::TNullTraceContextGuard guard;
            auto syncError = TError(
                NRpc::EErrorCode::Unavailable,
                "Failed to synchronize with cell %v since it has disconnected",
                mailbox->GetCellId());
            for (const auto& [messageId, syncPromise] : mailbox->SyncRequests()) {
                syncPromise.Set(syncError);
            }
        }

        mailbox->SyncRequests().clear();
        mailbox->SetConnected(false);

        auto doSetDisconnected = [] (TMailbox* mailbox) {
            mailbox->SetPostInProgress(false);
            mailbox->SetFirstInFlightOutcomingMessageId(mailbox->GetFirstOutcomingMessageId());
            mailbox->SetInFlightOutcomingMessageCount(0);
        };

        doSetDisconnected(mailbox);
        for (auto [avenueEndpointId, avenueMailbox] : mailbox->RegisteredAvenues()) {
            doSetDisconnected(avenueMailbox);
        }

        TDelayedExecutor::CancelAndClear(mailbox->IdlePostCookie());

        YT_LOG_INFO("Mailbox disconnected (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            mailbox->GetCellId());
    }

    void InitSyncBatchers()
    {
        auto guard = WriterGuard(CellToIdToSyncBatcherLock_);
        SyncBatchersInitialized_ = true;
    }

    void CancelSyncBatchers(const TError& error)
    {
        decltype(CellToIdToSyncBatcher_) cellToIdToBatcher;
        {
            auto guard = WriterGuard(CellToIdToSyncBatcherLock_);
            std::swap(cellToIdToBatcher, CellToIdToSyncBatcher_);
            SyncBatchersInitialized_ = false;
        }

        for (const auto& [cellId, batcher] : cellToIdToBatcher) {
            batcher->Cancel(error);
        }
    }

    void ResetMailboxes()
    {
        CancelSyncBatchers(TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped"));

        for (auto [id, mailbox] : CellMailboxMap_) {
            SetMailboxDisconnected(mailbox);
            mailbox->SetNextTransientIncomingMessageId(-1);
            mailbox->SetAcknowledgeInProgress(false);
            mailbox->SetCachedChannel(nullptr);
            mailbox->SetPostBatchingCookie(nullptr);
        }

        for (auto [id, mailbox] : AvenueMailboxMap_) {
            mailbox->SetNextTransientIncomingMessageId(-1);
            mailbox->SetAcknowledgeInProgress(false);
        }

        ReadOnlyCheckExecutor_.Reset();
    }

    void PrepareLeaderMailboxes()
    {
        for (auto [id, mailbox] : CellMailboxMap_) {
            mailbox->SetNextTransientIncomingMessageId(mailbox->GetNextPersistentIncomingMessageId());
        }

        for (auto [id, mailbox] : AvenueMailboxMap_) {
            mailbox->SetNextTransientIncomingMessageId(mailbox->GetNextPersistentIncomingMessageId());
        }
    }


    void ValidateCellNotRemoved(TCellId cellId)
    {
        if (RemovedCellIds_.contains(cellId)) {
            THROW_ERROR_EXCEPTION("Cell %v is removed",
                cellId);
        }
    }


    void SchedulePeriodicPing(TCellMailbox* mailbox)
    {
        TDelayedExecutor::Submit(
            BIND_NO_PROPAGATE(&THiveManager::OnPeriodicPingTick, MakeWeak(this), mailbox->GetCellId())
                .Via(EpochAutomatonInvoker_),
            Config_->PingPeriod);
    }

    void ReconnectMailboxes()
    {
        InitSyncBatchers();

        for (auto [id, mailbox] : CellMailboxMap_) {
            YT_VERIFY(!mailbox->GetConnected());
            SendPeriodicPing(mailbox);
        }

        ReadOnlyCheckExecutor_ = New<TPeriodicExecutor>(
            EpochAutomatonInvoker_,
            BIND_NO_PROPAGATE(&THiveManager::OnReadOnlyCheck, MakeWeak(this)),
            ReadOnlyCheckPeriod);
        ReadOnlyCheckExecutor_->Start();
    }

    void OnPeriodicPingTick(TCellId cellId)
    {
        auto* mailbox = FindCellMailbox(cellId);
        if (!mailbox) {
            return;
        }

        SendPeriodicPing(mailbox);
    }

    void SendPeriodicPing(TCellMailbox* mailbox)
    {
        auto cellId = mailbox->GetCellId();

        if (IsLeader() && CellDirectory_->IsCellUnregistered(cellId)) {
            NHiveServer::NProto::TReqUnregisterMailbox req;
            ToProto(req.mutable_cell_id(), cellId);
            YT_UNUSED_FUTURE(CreateUnregisterMailboxMutation(req)
                ->CommitAndLog(Logger));
            return;
        }

        if (mailbox->GetConnected()) {
            SchedulePeriodicPing(mailbox);
            return;
        }

        auto channel = FindMailboxChannel(mailbox);
        if (!channel) {
            // Let's register a dummy descriptor so as to ask about it during the next sync.
            CellDirectory_->RegisterCell(cellId);
            SchedulePeriodicPing(mailbox);
            return;
        }

        YT_LOG_DEBUG("Sending periodic ping (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            mailbox->GetCellId());

        NTracing::TNullTraceContextGuard guard;

        THiveServiceProxy proxy(std::move(channel));

        auto req = proxy.Ping();
        req->SetTimeout(Config_->PingRpcTimeout);
        ToProto(req->mutable_src_cell_id(), SelfCellId_);

        req->Invoke().Subscribe(
            BIND(&THiveManager::OnPeriodicPingResponse, MakeStrong(this), mailbox->GetCellId())
                .Via(EpochAutomatonInvoker_));
    }

    void OnPeriodicPingResponse(TCellId cellId, const THiveServiceProxy::TErrorOrRspPingPtr& rspOrError)
    {
        auto* mailbox = FindCellMailbox(cellId);
        if (!mailbox) {
            return;
        }

        SchedulePeriodicPing(mailbox);

        if (!rspOrError.IsOK()) {
            YT_LOG_DEBUG(rspOrError, "Periodic ping failed (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                mailbox->GetCellId());
            return;
        }

        const auto& rsp = rspOrError.Value();
        auto lastOutcomingMessageId = rsp->last_outcoming_message_id();

        YT_LOG_DEBUG("Periodic ping succeeded (SrcCellId: %v, DstCellId: %v, LastOutcomingMessageId: %v)",
            SelfCellId_,
            mailbox->GetCellId(),
            lastOutcomingMessageId);

        SetMailboxConnected(mailbox);
    }


    TIntrusivePtr<TAsyncBatcher<void>> GetOrCreateSyncBatcher(TCellId cellId)
    {
        {
            auto readerGuard = ReaderGuard(CellToIdToSyncBatcherLock_);
            auto it = CellToIdToSyncBatcher_.find(cellId);
            if (it != CellToIdToSyncBatcher_.end()) {
                return it->second;
            }
        }

        auto batcher = New<TAsyncBatcher<void>>(
            BIND_NO_PROPAGATE(&THiveManager::DoSyncWith, MakeWeak(this), cellId),
            Config_->SyncDelay);

        {
            auto writerGuard = WriterGuard(CellToIdToSyncBatcherLock_);
            if (SyncBatchersInitialized_) {
                auto it = CellToIdToSyncBatcher_.emplace(cellId, std::move(batcher)).first;
                return it->second;
            } else {
                return nullptr;
            }
        }
    }

    static TFuture<void> DoSyncWith(const TWeakPtr<THiveManager>& weakThis, TCellId cellId)
    {
        auto this_ = weakThis.Lock();
        if (!this_) {
            return MakeFuture(TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped"));
        }

        return this_->DoSyncWithCore(cellId);
    }

    TFuture<void> DoSyncWithCore(TCellId cellId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto channel = CellDirectory_->FindChannelByCellId(cellId, EPeerKind::Leader);
        if (!channel) {
            return MakeFuture(TError(
                NRpc::EErrorCode::Unavailable,
                "Cannot synchronize with cell %v since it is not connected",
                cellId));
        }

        YT_LOG_DEBUG("Synchronizing with another instance (SrcCellId: %v, DstCellId: %v)",
            cellId,
            SelfCellId_);

        NTracing::TNullTraceContextGuard guard;

        auto it = PerCellSyncTimeCounter_.find(cellId);
        if (it == PerCellSyncTimeCounter_.end()) {
            auto profiler = Profiler_
                .WithTag("source_cell_id", ToString(cellId));

            it = EmplaceOrCrash(PerCellSyncTimeCounter_, cellId, profiler.TimeCounter("/cell_sync_time"));
        }

        TWallTimer timer;
        THiveServiceProxy proxy(std::move(channel));

        auto req = proxy.Ping();
        req->SetTimeout(Config_->PingRpcTimeout);
        ToProto(req->mutable_src_cell_id(), SelfCellId_);

        return req->Invoke()
            .Apply(
                BIND(&THiveManager::OnSyncPingResponse, MakeStrong(this), cellId)
                    .AsyncViaGuarded(
                        GuardedAutomatonInvoker_,
                        TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped")))
            .WithTimeout(Config_->SyncTimeout)
            // NB: Many subscribers are typically waiting for the sync to complete.
            // Make sure the promise is set in a large thread pool.
            .Apply(
                BIND([syncTimeCounter = it->second, timer = std::move(timer)] (const TError& error) {
                    syncTimeCounter.Add(timer.GetElapsedTime());
                    error.ThrowOnError();
                })
                    .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
    }

    TFuture<void> OnSyncPingResponse(TCellId cellId, const THiveServiceProxy::TErrorOrRspPingPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (!rspOrError.IsOK()) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Failed to synchronize with cell %v",
                cellId)
                << rspOrError;
        }

        auto* mailbox = GetMailboxOrThrow(cellId)->AsCell();
        if (!mailbox->GetConnected()) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Unable to synchronize with cell %v since it is not connected",
                cellId);
        }

        const auto& rsp = rspOrError.Value();
        if (!rsp->has_last_outcoming_message_id()) {
            YT_LOG_DEBUG("Remote instance has no mailbox; no synchronization needed (SrcCellId: %v, DstCellId: %v)",
                cellId,
                SelfCellId_);
            return VoidFuture;
        }

        auto messageId = rsp->last_outcoming_message_id();
        if (messageId < mailbox->GetNextPersistentIncomingMessageId()) {
            YT_LOG_DEBUG("Already synchronized with remote instance (SrcCellId: %v, DstCellId: %v, "
                "SyncMessageId: %v, NextPersistentIncomingMessageId: %v)",
                cellId,
                SelfCellId_,
                messageId,
                mailbox->GetNextPersistentIncomingMessageId());
            return VoidFuture;
        }

        YT_LOG_DEBUG("Waiting for synchronization with remote instance (SrcCellId: %v, DstCellId: %v, "
            "SyncMessageId: %v, NextPersistentIncomingMessageId: %v)",
            cellId,
            SelfCellId_,
            messageId,
            mailbox->GetNextPersistentIncomingMessageId());

        return RegisterSyncRequest(mailbox, messageId);
    }

    TFuture<void> RegisterSyncRequest(TCellMailbox* mailbox, TMessageId messageId)
    {
        auto& syncRequests = mailbox->SyncRequests();

        auto it = syncRequests.find(messageId);
        if (it != syncRequests.end()) {
            return it->second.ToFuture();
        }

        auto promise = NewPromise<void>();
        YT_VERIFY(syncRequests.emplace(messageId, promise).second);
        return promise.ToFuture();
    }

    void FlushSyncRequests(TCellMailbox* mailbox)
    {
        NTracing::TNullTraceContextGuard guard;
        auto& syncRequests = mailbox->SyncRequests();
        while (!syncRequests.empty()) {
            auto it = syncRequests.begin();
            auto messageId = it->first;
            if (messageId >= mailbox->GetNextPersistentIncomingMessageId()) {
                break;
            }

            YT_LOG_DEBUG("Synchronization complete (SrcCellId: %v, DstCellId: %v, MessageId: %v)",
                SelfCellId_,
                mailbox->GetCellId(),
                messageId);

            it->second.Set();
            syncRequests.erase(it);
        }
    }

    void OnIdlePostOutcomingMessages(TCellId cellId)
    {
        TWallTimer timer;
        auto finally = Finally([&] {
            SyncPostingTimeCounter_.Add(timer.GetElapsedTime());
        });

        auto* mailbox = FindCellMailbox(cellId);
        if (!mailbox) {
            return;
        }

        PostOutcomingMessages(mailbox, true);
    }

    void SchedulePostOutcomingMessages(TMailbox* mailboxBase)
    {
        if (mailboxBase->IsAvenue()) {
            if (auto* cellMailbox = mailboxBase->AsAvenue()->GetCellMailbox()) {
                SchedulePostOutcomingMessages(cellMailbox);
            }

            return;
        }

        auto* mailbox = mailboxBase->AsCell();

        if (mailbox->GetPostBatchingCookie()) {
            return;
        }

        if (!IsLeader()) {
            return;
        }

        NTracing::TNullTraceContextGuard guard;

        mailbox->SetPostBatchingCookie(TDelayedExecutor::Submit(
            BIND_NO_PROPAGATE([this, this_ = MakeStrong(this), cellId = mailbox->GetCellId()] {
                TWallTimer timer;
                auto finally = Finally([&] {
                    SyncPostingTimeCounter_.Add(timer.GetElapsedTime());
                });

                auto* mailbox = FindCellMailbox(cellId);
                if (!mailbox) {
                    return;
                }

                mailbox->SetPostBatchingCookie(nullptr);
                PostOutcomingMessages(mailbox, false);
            }).Via(EpochAutomatonInvoker_),
            Config_->PostBatchingPeriod));
    }

    void PostOutcomingMessages(TCellMailbox* mailbox, bool allowIdle)
    {
        if (!IsLeader()) {
            return;
        }

        if (!mailbox->GetConnected()) {
            return;
        }

        if (mailbox->GetInFlightOutcomingMessageCount() > 0) {
            return;
        }

        if (mailbox->GetPostInProgress()) {
            return;
        }

        NTracing::TNullTraceContextGuard guard;

        bool hasOutcomingMessages = false;

        auto checkOutcomingMessages = [&] (TMailbox* mailbox) {
            if (mailbox->GetInFlightOutcomingMessageCount() > 0) {
                return;
            }

            auto firstInFlightOutcomingMessageId = mailbox->GetFirstInFlightOutcomingMessageId();
            auto firstOutcomingMessageId = mailbox->GetFirstOutcomingMessageId();
            const auto& outcomingMessages = mailbox->OutcomingMessages();

            YT_VERIFY(firstInFlightOutcomingMessageId >= firstOutcomingMessageId);
            YT_VERIFY(firstInFlightOutcomingMessageId <= firstOutcomingMessageId + std::ssize(outcomingMessages));

            if (firstInFlightOutcomingMessageId != mailbox->GetFirstOutcomingMessageId() + std::ssize(outcomingMessages)) {
                hasOutcomingMessages = true;
            }
        };

        checkOutcomingMessages(mailbox);
        for (auto [id, avenueMailbox] : mailbox->ActiveAvenues()) {
            checkOutcomingMessages(avenueMailbox);
        }

        auto dstCellId = mailbox->GetCellId();

        TDelayedExecutor::CancelAndClear(mailbox->IdlePostCookie());
        if (!allowIdle && !hasOutcomingMessages) {
            mailbox->IdlePostCookie() = TDelayedExecutor::Submit(
                BIND_NO_PROPAGATE(&THiveManager::OnIdlePostOutcomingMessages, MakeWeak(this), dstCellId)
                    .Via(EpochAutomatonInvoker_),
                Config_->IdlePostPeriod);
            return;
        }

        auto channel = FindMailboxChannel(mailbox);
        if (!channel) {
            return;
        }

        struct TEnvelope
        {
            TEndpointId SrcEndpointId;
            i64 FirstMessageId;
            std::vector<TCellMailbox::TOutcomingMessage> MessagesToPost;
        };

        TEnvelope cellEnvelope;
        std::vector<TEnvelope> avenueEnvelopes;
        std::vector<TAvenueEndpointId> avenueSrcIds;

        i64 messageBytesToPost = 0;
        int messageCountToPost = 0;

        auto isOverflown = [&] {
            return !(messageCountToPost < Config_->MaxMessagesPerPost &&
                messageBytesToPost < Config_->MaxBytesPerPost);
        };

        auto fillEnvelope = [&] (TMailbox* mailbox, TEnvelope* envelope) {
            int localMessageCountToPost = 0;
            auto firstInFlightOutcomingMessageId = mailbox->GetFirstInFlightOutcomingMessageId();
            auto firstOutcomingMessageId = mailbox->GetFirstOutcomingMessageId();
            auto& outcomingMessages = mailbox->OutcomingMessages();

            int currentMessageIndex = firstInFlightOutcomingMessageId - firstOutcomingMessageId;
            while (currentMessageIndex < std::ssize(outcomingMessages) && !isOverflown()) {
                const auto& message = outcomingMessages[currentMessageIndex];
                envelope->MessagesToPost.push_back(message);
                messageBytesToPost += message.SerializedMessage->Data.size();
                ++messageCountToPost;
                ++currentMessageIndex;
                ++localMessageCountToPost;
            }

            envelope->FirstMessageId = firstInFlightOutcomingMessageId;

            mailbox->SetInFlightOutcomingMessageCount(localMessageCountToPost);
            mailbox->SetPostInProgress(true);
        };

        cellEnvelope.SrcEndpointId = SelfCellId_;
        fillEnvelope(mailbox, &cellEnvelope);
        int cellMessageCountToPost = messageCountToPost;

        for (auto [avenueEndpointId, avenueMailbox] : mailbox->ActiveAvenues()) {
            if (isOverflown()) {
                break;
            }

            if (avenueMailbox->GetInFlightOutcomingMessageCount() > 0) {
                continue;
            }

            auto srcEndpointId = GetSiblingAvenueEndpointId(avenueEndpointId);
            avenueEnvelopes.push_back({
                .SrcEndpointId = srcEndpointId,
            });
            avenueSrcIds.push_back(srcEndpointId);

            fillEnvelope(avenueMailbox, &avenueEnvelopes.back());
        }

        if (messageCountToPost == 0) {
            YT_LOG_DEBUG("Checking mailbox synchronization (SrcCellId: %v, DstCellId: %v, AvenueEndpointIds: %v)",
                SelfCellId_,
                dstCellId,
                MakeFormattableView(
                    avenueEnvelopes,
                    [] (auto* builder, const auto& envelope) {
                        builder->AppendFormat("%v", GetSiblingAvenueEndpointId(envelope.SrcEndpointId));
                    }
                ));
        } else {
            YT_LOG_DEBUG("Posting reliable outcoming messages "
                "(SrcCellId: %v, DstCellId: %v, MessageIds: %v-%v, Avenues: %v)",
                SelfCellId_,
                dstCellId,
                cellEnvelope.FirstMessageId,
                cellEnvelope.FirstMessageId + cellMessageCountToPost - 1,
                MakeFormattableView(
                    avenueEnvelopes,
                    [] (auto* builder, const auto& envelope) {
                        builder->AppendFormat("%v:", GetSiblingAvenueEndpointId(envelope.SrcEndpointId));
                        if (envelope.MessagesToPost.empty()) {
                            builder->AppendString("sync");
                        } else {
                            builder->AppendFormat(
                                "%v-%v",
                                envelope.FirstMessageId,
                                envelope.FirstMessageId + ssize(envelope.MessagesToPost) - 1);
                        }
                    }
                ));
        }

        NRpc::TDispatcher::Get()->GetHeavyInvoker()->Invoke(BIND(
            [
                =,
                this,
                this_ = MakeStrong(this),
                cellEnvelope = std::move(cellEnvelope),
                avenueEnvelopes = std::move(avenueEnvelopes),
                epochAutomatonInvoker = EpochAutomatonInvoker_
            ] () mutable {
                TWallTimer timer;
                auto finally = Finally([&] {
                    AsyncPostingTimeCounter_.Add(timer.GetElapsedTime());
                });

                THiveServiceProxy proxy(std::move(channel));

                auto req = proxy.PostMessages();
                req->SetTimeout(Config_->PostRpcTimeout);

                auto fillSubrequest = [] (auto* req, const auto& envelope) {
                    ToProto(req->mutable_src_endpoint_id(), envelope.SrcEndpointId);
                    req->set_first_message_id(envelope.FirstMessageId);
                    for (const auto& message : envelope.MessagesToPost) {
                        auto* protoMessage = req->add_messages();
                        protoMessage->set_type(message.SerializedMessage->Type);
                        protoMessage->set_data(message.SerializedMessage->Data);
                        if (message.TraceContext) {
                            ToProto(protoMessage->mutable_tracing_ext(), message.TraceContext);
                        }
                        protoMessage->set_logical_time(message.Time.Underlying());
                    }
                };

                fillSubrequest(req.Get(), cellEnvelope);
                for (const auto& avenueEnvelope : avenueEnvelopes) {
                    fillSubrequest(req->add_avenue_subrequests(), avenueEnvelope);
                }

                req->Invoke().Subscribe(
                    BIND(&THiveManager::OnPostMessagesResponse, MakeStrong(this), dstCellId, Passed(std::move(avenueSrcIds)))
                        .Via(epochAutomatonInvoker));
            }));
    }

    void OnPostMessagesResponse(
        TCellId cellId,
        std::vector<TAvenueEndpointId> avenueSrcIds,
        const THiveServiceProxy::TErrorOrRspPostMessagesPtr& rspOrError)
    {
        TWallTimer timer;
        auto finally = Finally([&] {
            SyncPostingTimeCounter_.Add(timer.GetElapsedTime());
        });

        auto* mailbox = FindCellMailbox(cellId);
        if (!mailbox) {
            // All relevant avenue mailboxes are unbound, posting has been already cancelled.
            return;
        }

        if (!mailbox->GetPostInProgress()) {
            // Posting has been already cancelled for all relevant avenue mailboxes.
            return;
        }

        DoPostMessagesResponse(mailbox, rspOrError);
        for (auto [index, srcEndpointId] : Enumerate(avenueSrcIds)) {
            auto dstEndpointId = GetSiblingAvenueEndpointId(srcEndpointId);
            DoPostMessagesResponse(mailbox, rspOrError, dstEndpointId, index);
        }
    }

    void DoPostMessagesResponse(
        TCellMailbox* cellMailbox,
        const THiveServiceProxy::TErrorOrRspPostMessagesPtr& rspOrError,
        TAvenueEndpointId dstEndpointId = {},
        std::optional<int> avenueSubrequestIndex = {})
    {
        TMailbox* mailbox;
        if (avenueSubrequestIndex) {
            if (auto* avenueMailbox = FindAvenueMailbox(dstEndpointId)) {
                mailbox = avenueMailbox;
            } else {
                return;
            }
        } else {
            mailbox = cellMailbox;
        }

        if (!mailbox->GetPostInProgress()) {
            return;
        }

        mailbox->SetInFlightOutcomingMessageCount(0);
        mailbox->SetPostInProgress(false);

        if (rspOrError.GetCode() == NHiveClient::EErrorCode::MailboxNotCreatedYet) {
            YT_VERIFY(mailbox->IsCell());
            YT_LOG_DEBUG(rspOrError, "Mailbox is not created yet; will retry (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                cellMailbox->GetCellId());
            SchedulePostOutcomingMessages(cellMailbox);
            return;
        }

        if (!rspOrError.IsOK()) {
            if (mailbox->IsCell()) {
                YT_LOG_DEBUG(rspOrError, "Failed to post reliable outcoming messages (SrcCellId: %v, DstCellId: %v)",
                    SelfCellId_,
                    cellMailbox->GetCellId());
                MaybeDisconnectMailboxOnError(cellMailbox, rspOrError);
            }
            return;
        }

        const auto& rsp = avenueSubrequestIndex
            ? rspOrError.Value()->avenue_subresponses(*avenueSubrequestIndex)
            : *rspOrError.Value();

        if (rsp.next_transient_incoming_message_id() == -1) {
            YT_LOG_DEBUG("Destination mailbox does not exist, post cancelled (%v)",
                FormatOutgoingMailboxEndpoints(mailbox));
            return;
        }

        auto nextPersistentIncomingMessageId = rsp.next_persistent_incoming_message_id();
        auto nextTransientIncomingMessageId = rsp.next_transient_incoming_message_id();
        YT_LOG_DEBUG("Outcoming reliable messages posted (%v, "
            "NextPersistentIncomingMessageId: %v, NextTransientIncomingMessageId: %v)",
            FormatOutgoingMailboxEndpoints(mailbox),
            nextPersistentIncomingMessageId,
            nextTransientIncomingMessageId);

        if (!HandlePersistentIncomingMessages(mailbox, nextPersistentIncomingMessageId)) {
            return;
        }

        if (!HandleTransientIncomingMessages(mailbox, nextTransientIncomingMessageId)) {
            return;
        }

        SchedulePostOutcomingMessages(mailbox);
    }

    void OnSendMessagesResponse(TCellId cellId, const THiveServiceProxy::TErrorOrRspSendMessagesPtr& rspOrError)
    {
        TWallTimer timer;
        auto finally = Finally([&] {
            SyncPostingTimeCounter_.Add(timer.GetElapsedTime());
        });

        auto* mailbox = FindCellMailbox(cellId);
        if (!mailbox) {
            return;
        }

        if (!rspOrError.IsOK()) {
            YT_LOG_DEBUG(rspOrError, "Failed to send unreliable outcoming messages (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                mailbox->GetCellId());
            MaybeDisconnectMailboxOnError(mailbox, rspOrError);
            return;
        }

        YT_LOG_DEBUG("Outcoming unreliable messages sent successfully (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            mailbox->GetCellId());
    }


    std::unique_ptr<TMutation> CreateAcknowledgeMessagesMutation(const NHiveServer::NProto::TReqAcknowledgeMessages& req)
    {
        return CreateMutation(
            HydraManager_,
            req,
            &THiveManager::HydraAcknowledgeMessages,
            this);
    }

    std::unique_ptr<TMutation> CreatePostMessagesMutation(const NHiveClient::NProto::TReqPostMessages& request)
    {
        return CreateMutation(
            HydraManager_,
            request,
            &THiveManager::HydraPostMessages,
            this);
    }

    std::unique_ptr<TMutation> CreateSendMessagesMutation(const TCtxSendMessagesPtr& context)
    {
        return CreateMutation(
            HydraManager_,
            context,
            &THiveManager::HydraSendMessages,
            this);
    }

    std::unique_ptr<TMutation> CreateRegisterMailboxMutation(const NHiveServer::NProto::TReqRegisterMailbox& req)
    {
        return CreateMutation(
            HydraManager_,
            req,
            &THiveManager::HydraRegisterMailbox,
            this);
    }

    std::unique_ptr<TMutation> CreateUnregisterMailboxMutation(const NHiveServer::NProto::TReqUnregisterMailbox& req)
    {
        return CreateMutation(
            HydraManager_,
            req,
            &THiveManager::HydraUnregisterMailbox,
            this);
    }


    bool CheckRequestedMessageIdAgainstMailbox(TMailbox* mailbox, TMessageId requestedMessageId)
    {
        if (requestedMessageId < mailbox->GetFirstOutcomingMessageId()) {
            YT_LOG_ALERT("Destination is out of sync: requested to receive already truncated messages (%v, "
                "RequestedMessageId: %v, FirstOutcomingMessageId: %v)",
                FormatOutgoingMailboxEndpoints(mailbox),
                requestedMessageId,
                mailbox->GetFirstOutcomingMessageId());
            SetMailboxDisconnected(mailbox);
            return false;
        }

        if (requestedMessageId > mailbox->GetFirstOutcomingMessageId() + std::ssize(mailbox->OutcomingMessages())) {
            YT_LOG_ALERT("Destination is out of sync: requested to receive nonexisting messages (%v, "
                "RequestedMessageId: %v, FirstOutcomingMessageId: %v, OutcomingMessageCount: %v)",
                FormatOutgoingMailboxEndpoints(mailbox),
                requestedMessageId,
                mailbox->GetFirstOutcomingMessageId(),
                mailbox->OutcomingMessages().size());
            SetMailboxDisconnected(mailbox);
            return false;
        }

        return true;
    }

    bool HandlePersistentIncomingMessages(TMailbox* mailbox, TMessageId nextPersistentIncomingMessageId)
    {
        if (!CheckRequestedMessageIdAgainstMailbox(mailbox, nextPersistentIncomingMessageId)) {
            return false;
        }

        if (mailbox->GetAcknowledgeInProgress()) {
            return true;
        }

        if (nextPersistentIncomingMessageId == mailbox->GetFirstOutcomingMessageId()) {
            return true;
        }

        NHiveServer::NProto::TReqAcknowledgeMessages req;
        ToProto(req.mutable_cell_id(), mailbox->GetEndpointId());
        req.set_next_persistent_incoming_message_id(nextPersistentIncomingMessageId);

        mailbox->SetAcknowledgeInProgress(true);

        YT_LOG_DEBUG("Committing reliable messages acknowledgement (%v, "
            "MessageIds: %v-%v)",
            FormatOutgoingMailboxEndpoints(mailbox),
            mailbox->GetFirstOutcomingMessageId(),
            nextPersistentIncomingMessageId - 1);

        YT_UNUSED_FUTURE(CreateAcknowledgeMessagesMutation(req)
            ->CommitAndLog(Logger));

        return true;
    }

    bool HandleTransientIncomingMessages(TMailbox* mailbox, TMessageId nextTransientIncomingMessageId)
    {
        if (!CheckRequestedMessageIdAgainstMailbox(mailbox, nextTransientIncomingMessageId)) {
            return false;
        }

        mailbox->SetFirstInFlightOutcomingMessageId(nextTransientIncomingMessageId);
        return true;
    }


    void ApplyReliableIncomingMessages(TMailbox* mailbox, const NHiveClient::NProto::TReqPostMessages* req)
    {
        for (int index = 0; index < req->messages_size(); ++index) {
            auto messageId = req->first_message_id() + index;
            ApplyReliableIncomingMessage(mailbox, messageId, req->messages(index));
        }
    }

    void ApplyReliableIncomingMessage(TMailbox* mailbox, TMessageId messageId, const TEncapsulatedMessage& message)
    {
        if (messageId != mailbox->GetNextPersistentIncomingMessageId()) {
            YT_LOG_ALERT("Attempt to apply an out-of-order message (%v, "
                "ExpectedMessageId: %v, ActualMessageId: %v, MutationType: %v)",
                FormatIncomingMailboxEndpoints(mailbox),
                mailbox->GetNextPersistentIncomingMessageId(),
                messageId,
                message.type());
            return;
        }

        std::optional<TTraceContextGuard> traceContextGuard;
        if (message.has_tracing_ext() && IsLeader()) {
            auto traceContext = NTracing::TTraceContext::NewChildFromRpc(
                message.tracing_ext(),
                ConcatToString(TStringBuf("HiveManager:"), message.type()));
            traceContextGuard.emplace(std::move(traceContext));
        }
        auto logicalTime = LamportClock_->Tick(TLogicalTime(message.logical_time()));

        auto* mutationContext = GetCurrentMutationContext();
        YT_LOG_DEBUG("Applying reliable incoming message (%v, "
            "MessageId: %v, MutationType: %v, LogicalTime: %v, SequenceNumber: %v)",
            FormatIncomingMailboxEndpoints(mailbox),
            messageId,
            message.type(),
            logicalTime,
            mutationContext->GetSequenceNumber());

        ApplyMessage(message, mailbox->GetEndpointId());

        mailbox->SetNextPersistentIncomingMessageId(messageId + 1);

        if (mailbox->IsCell()) {
            FlushSyncRequests(mailbox->AsCell());
        }
    }

    void ApplyUnreliableIncomingMessages(TCellMailbox* mailbox, const NHiveClient::NProto::TReqSendMessages* req)
    {
        for (const auto& message : req->messages()) {
            ApplyUnreliableIncomingMessage(mailbox, message);
        }
    }

    void ApplyUnreliableIncomingMessage(TCellMailbox* mailbox, const TEncapsulatedMessage& message)
    {
        YT_LOG_DEBUG("Applying unreliable incoming message (SrcCellId: %v, DstCellId: %v, MutationType: %v)",
            mailbox->GetCellId(),
            SelfCellId_,
            message.type());
        ApplyMessage(message, mailbox->GetCellId());
    }

    void ApplyMessage(const TEncapsulatedMessage& message, TEndpointId endpointId)
    {
        TMutationRequest request{
            .Reign = GetCurrentMutationContext()->Request().Reign,
            .Type = message.type(),
            .Data = TSharedRef::FromString(message.data())
        };

        TMutationContext mutationContext(GetCurrentMutationContext(), &request);
        TMutationContextGuard mutationContextGuard(&mutationContext);

        THiveMutationGuard hiveMutationGuard(endpointId);

        static_cast<IAutomaton*>(Automaton_)->ApplyMutation(&mutationContext);
    }

    void OnAvenueDirectoryEndpointUpdated(TAvenueEndpointId endpointId, TCellId newCellId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (auto* mailbox = FindAvenueMailbox(endpointId)) {
            UpdateAvenueCellConnection(mailbox, newCellId);
        }
    }

    void UpdateAvenueCellConnection(TAvenueMailbox* avenueMailbox, TCellId cellId)
    {
        TCellMailbox* cellMailbox = nullptr;

        if (cellId) {
            cellMailbox = GetCellMailbox(cellId);
        }

        UpdateAvenueCellConnection(avenueMailbox, cellMailbox);
    }

    void UpdateAvenueCellConnection(TAvenueMailbox* avenueMailbox, TCellMailbox* cellMailbox)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (cellMailbox == avenueMailbox->GetCellMailbox()) {
            return;
        }

        avenueMailbox->SetPostInProgress(false);
        avenueMailbox->SetFirstInFlightOutcomingMessageId(avenueMailbox->GetFirstOutcomingMessageId());
        avenueMailbox->SetInFlightOutcomingMessageCount(0);

        if (auto* formerCellMailbox = avenueMailbox->GetCellMailbox()) {
            EraseOrCrash(formerCellMailbox->RegisteredAvenues(), avenueMailbox->GetEndpointId());
            YT_LOG_DEBUG(
                "Avenue disconnected from cell (AvenueEndpointId: %v, CellId: %v)",
                avenueMailbox->GetEndpointId(),
                formerCellMailbox ? formerCellMailbox->GetCellId() : NullObjectId);
        }

        if (avenueMailbox->IsActive()) {
            if (auto* formerCellMailbox = avenueMailbox->GetCellMailbox()) {
                formerCellMailbox->ActiveAvenues().erase(avenueMailbox->GetEndpointId());
            }

            if (cellMailbox) {
                cellMailbox->ActiveAvenues().emplace(
                    avenueMailbox->GetEndpointId(),
                    avenueMailbox);
                YT_LOG_DEBUG(
                    "Active avenue registered at cell (AvenueEndpointId: %v, CellId: %v)",
                    avenueMailbox->GetEndpointId(),
                    cellMailbox->GetCellId());
                SchedulePostOutcomingMessages(cellMailbox);
            }
        }

        if (!avenueMailbox->IsActive()) {
            if (auto* formerCellMailbox = avenueMailbox->GetCellMailbox()) {
                YT_VERIFY(!formerCellMailbox->ActiveAvenues().contains(avenueMailbox->GetEndpointId()));
            }
        }

        avenueMailbox->SetCellMailbox(cellMailbox);
        if (cellMailbox) {
            EmplaceOrCrash(
                cellMailbox->RegisteredAvenues(),
                avenueMailbox->GetEndpointId(),
                avenueMailbox);
            YT_LOG_DEBUG("Avenue connected to cell "
                "(AvenueEndpointId: %v, CellId: %v)",
                avenueMailbox->GetEndpointId(),
                cellMailbox->GetCellId());
        }
    }

    TString FormatIncomingMailboxEndpoints(TMailbox* mailbox) const
    {
        return FormatMailboxEndpoints(mailbox, /*outgoing*/ false);
    }

    TString FormatOutgoingMailboxEndpoints(TMailbox* mailbox) const
    {
        return FormatMailboxEndpoints(mailbox, /*outgoing*/ true);
    }

    TString FormatMailboxEndpoints(TMailbox* mailbox, bool outgoing) const
    {
        if (mailbox->IsCell()) {
            auto srcId = SelfCellId_;
            auto dstId = mailbox->GetEndpointId();

            if (!outgoing) {
                std::swap(srcId, dstId);
            }

            return Format("SrcCellId: %v, DstCellId: %v", srcId, dstId);
        } else {
            auto dstId = mailbox->GetEndpointId();
            auto srcId = GetSiblingAvenueEndpointId(dstId);

            if (!outgoing) {
                std::swap(srcId, dstId);
            }

            return Format("SrcEndpointId: %v, DstEndpointId: %v", srcId, dstId);
        }
    }

    void OnReadOnlyCheck()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (!HydraManager_->GetReadOnly()) {
            return;
        }

        YT_LOG_DEBUG("Hydra is read-only; canceling all synchronization requests");

        auto error = TError(NHydra::EErrorCode::ReadOnly, "Cannot synchronize with remote instance since Hydra is read-only");

        CancelSyncBatchers(error);

        for (auto [_, mailbox] : CellMailboxMap_) {
            auto requests = std::exchange(mailbox->SyncRequests(), {});
            for (const auto& [_, promise] : requests) {
                promise.Set(error);
            }
        }
    }


    // NB: Leader must wait until it is active before reconnecting mailboxes
    // since no commits are possible before this point.
    void OnLeaderActive() override
    {
        TCompositeAutomatonPart::OnLeaderActive();
        ReconnectMailboxes();
        PrepareLeaderMailboxes();
    }

    void OnStopLeading() override
    {
        TCompositeAutomatonPart::OnStopLeading();
        ResetMailboxes();
    }

    void OnFollowerRecoveryComplete() override
    {
        TCompositeAutomatonPart::OnFollowerRecoveryComplete();
        ReconnectMailboxes();
    }

    void OnStopFollowing() override
    {
        TCompositeAutomatonPart::OnStopFollowing();
        ResetMailboxes();
    }


    bool ValidateSnapshotVersion(int version) override
    {
        return version == 5 ||
            version == 6 || // COMPAT(ifsmirnov): Avenues.
            version == 7 ||
            false;
    }

    int GetCurrentSnapshotVersion() override
    {
        return 7;
    }


    void Clear() override
    {
        TCompositeAutomatonPart::Clear();

        CellMailboxMap_.Clear();
        AvenueMailboxMap_.Clear();

        {
            auto guard = WriterGuard(MailboxRuntimeDataMapLock_);
            MailboxRuntimeDataMap_.clear();
        }

        RemovedCellIds_.clear();
    }

    void SaveKeys(TSaveContext& context) const
    {
        CellMailboxMap_.SaveKeys(context);
        AvenueMailboxMap_.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context) const
    {
        CellMailboxMap_.SaveValues(context);
        AvenueMailboxMap_.SaveValues(context);
        Save(context, RemovedCellIds_);
        Save(context, *LamportClock_);
    }

    void LoadKeys(TLoadContext& context)
    {
        CellMailboxMap_.LoadKeys(context);
        // COMPAT(ifsmirnov)
        if (context.GetVersion() >= 6) {
            AvenueMailboxMap_.LoadKeys(context);
        }
    }

    void LoadValues(TLoadContext& context)
    {
        CellMailboxMap_.LoadValues(context);
        // COMPAT(ifsmirnov)
        if (context.GetVersion() >= 6) {
            AvenueMailboxMap_.LoadValues(context);
        }
        Load(context, RemovedCellIds_);
        // COMPAT(danilalexeev)
        if (context.GetVersion() >= 7) {
            Load(context, *LamportClock_);
        }

        {
            auto guard = WriterGuard(MailboxRuntimeDataMapLock_);
            MailboxRuntimeDataMap_.clear();
            for (auto [id, mailbox] : CellMailboxMap_) {
                EmplaceOrCrash(MailboxRuntimeDataMap_, id, mailbox->GetRuntimeData());
            }
            for (auto [id, mailbox] : AvenueMailboxMap_) {
                EmplaceOrCrash(MailboxRuntimeDataMap_, id, mailbox->GetRuntimeData());
            }
        }
    }

    int GetRemovedCellIdCount() const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return RemovedCellIds_.size();
    }

    TLogicalTime GetLamportTimestamp() const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return LamportClock_->GetTime();
    }

    IYPathServicePtr CreateOrchidService()
    {
        auto invoker = HydraManager_->CreateGuardedAutomatonInvoker(AutomatonInvoker_);
        return New<TCompositeMapService>()
            ->AddChild("cell_mailboxes", New<TMailboxOrchidService<TCellMailbox>>(
                MakeWeak(this),
                &THiveManager::CellMailboxes))
            ->AddChild("avenue_mailboxes", New<TMailboxOrchidService<TAvenueMailbox>>(
                MakeWeak(this),
                &THiveManager::AvenueMailboxes))
            ->AddChild("removed_cell_count", IYPathService::FromMethod(
                &THiveManager::GetRemovedCellIdCount,
                MakeWeak(this)))
            ->AddChild("lamport_timestamp", IYPathService::FromMethod(
                &THiveManager::GetLamportTimestamp,
                MakeWeak(this)))
            ->Via(invoker);
    }

    template <class TMailbox>
    class TMailboxOrchidService
        : public TVirtualMapBase
    {
    public:
        using TMailboxMapAccessor =
            const NHydra::TReadOnlyEntityMap<TMailbox>& (THiveManager::*)() const;

        TMailboxOrchidService(
            TWeakPtr<THiveManager> hiveManager,
            TMailboxMapAccessor mailboxMapAccessor)
            : Owner_(std::move(hiveManager))
            , MailboxMapAccessor_(mailboxMapAccessor)
        { }

        std::vector<TString> GetKeys(i64 limit) const override
        {
            std::vector<TString> keys;

            if (auto owner = Owner_.Lock()) {
                const auto& mailboxes = ((owner.Get())->*MailboxMapAccessor_)();
                keys.reserve(std::min(limit, std::ssize(mailboxes)));

                for (auto [id, mailbox] : mailboxes) {
                    if (std::ssize(keys) >= limit) {
                        break;
                    }
                    keys.push_back(ToString(id));
                }
            }

            return keys;
        }

        i64 GetSize() const override
        {
            if (auto owner = Owner_.Lock()) {
                return std::ssize(((owner.Get())->*MailboxMapAccessor_)());
            }
            return 0;
        }

        IYPathServicePtr FindItemService(TStringBuf key) const override
        {
            if (auto owner = Owner_.Lock()) {
                const auto& mailboxes = ((owner.Get())->*MailboxMapAccessor_)();
                if (auto* mailbox = mailboxes.Find(TEndpointId::FromString(key))) {
                    auto producer = BIND(&THiveManager::BuildMailboxOrchidYson, owner, mailbox);
                    return ConvertToNode(producer);
                }
            }
            return nullptr;
        }

    private:
        const TWeakPtr<THiveManager> Owner_;
        TMailboxMapAccessor MailboxMapAccessor_;
    };

    void BuildMailboxOrchidYson(const TMailbox* mailbox, IYsonConsumer* consumer) const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        consumer->OnBeginMap();

        BuildYsonMapFragmentFluently(consumer)
            .Item("acknowledge_in_progress").Value(mailbox->GetAcknowledgeInProgress())
            .Item("post_in_progress").Value(mailbox->GetPostInProgress())
            .Item("first_outcoming_message_id").Value(mailbox->GetFirstOutcomingMessageId())
            .Item("outcoming_message_count").Value(mailbox->OutcomingMessages().size())
            .Item("next_persistent_incoming_message_id").Value(mailbox->GetNextPersistentIncomingMessageId())
            .Item("next_transient_incoming_message_id").Value(mailbox->GetNextTransientIncomingMessageId())
            .Item("first_in_flight_outcoming_message_id").Value(mailbox->GetFirstInFlightOutcomingMessageId())
            .Item("in_flight_outcoming_message_count").Value(mailbox->GetInFlightOutcomingMessageCount());

        if (mailbox->IsCell()) {
            auto* cellMailbox = mailbox->AsCell();

            BuildYsonMapFragmentFluently(consumer)
                .Item("connected").Value(cellMailbox->GetConnected())
                .Item("registered_avenue_ids").DoListFor(
                    cellMailbox->RegisteredAvenues(),
                    [] (auto fluent, const auto& pair) {
                        fluent.Item().Value(pair.first);
                    })
                .Item("active_avenue_ids").DoListFor(
                    cellMailbox->ActiveAvenues(),
                    [] (auto fluent, const auto& pair) {
                        fluent.Item().Value(pair.first);
                    });
        }

        if (mailbox->IsAvenue()) {
            auto* avenueMailbox = mailbox->AsAvenue();

            auto* cellMailbox = avenueMailbox->GetCellMailbox();

            BuildYsonMapFragmentFluently(consumer)
                .Item("cell_id").Value(cellMailbox ? cellMailbox->GetCellId() : TCellId{});
        }

        consumer->OnEndMap();
    }

};

DEFINE_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS(THiveManager, CellMailbox, CellMailboxes, TCellMailbox, CellMailboxMap_);
DEFINE_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS(THiveManager, AvenueMailbox, AvenueMailboxes, TAvenueMailbox, AvenueMailboxMap_);

////////////////////////////////////////////////////////////////////////////////

IHiveManagerPtr CreateHiveManager(
    THiveManagerConfigPtr config,
    ICellDirectoryPtr cellDirectory,
    IAvenueDirectoryPtr avenueDirectory,
    TCellId selfCellId,
    IInvokerPtr automatonInvoker,
    IHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton,
    IUpstreamSynchronizerPtr upstreamSynchronizer,
    IAuthenticatorPtr authenticator)
{
    return New<THiveManager>(
        std::move(config),
        std::move(cellDirectory),
        std::move(avenueDirectory),
        selfCellId,
        std::move(automatonInvoker),
        std::move(hydraManager),
        std::move(automaton),
        std::move(upstreamSynchronizer),
        std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
