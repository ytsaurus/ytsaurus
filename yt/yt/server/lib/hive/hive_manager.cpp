#include "hive_manager.h"
#include "config.h"
#include "mailbox.h"
#include "helpers.h"
#include "private.h"

#include <yt/server/lib/election/election_manager.h>

#include <yt/server/lib/hydra/composite_automaton.h>
#include <yt/server/lib/hydra/hydra_manager.h>
#include <yt/server/lib/hydra/hydra_service.h>
#include <yt/server/lib/hydra/mutation.h>
#include <yt/server/lib/hydra/mutation_context.h>

#include <yt/ytlib/hive/cell_directory.h>
#include <yt/ytlib/hive/hive_service_proxy.h>

#include <yt/ytlib/hydra/config.h>
#include <yt/ytlib/hydra/peer_channel.h>

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/fls.h>
#include <yt/core/concurrency/async_batcher.h>

#include <yt/core/net/local_address.h>

#include <yt/core/rpc/proto/rpc.pb.h>
#include <yt/core/rpc/server.h>
#include <yt/core/rpc/service_detail.h>

#include <yt/core/tracing/trace_context.h>

#include <yt/core/ytree/fluent.h>

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

using NYT::ToProto;
using NYT::FromProto;

using NHiveClient::NProto::TEncapsulatedMessage;

////////////////////////////////////////////////////////////////////////////////

static const auto& Profiler = HiveServerProfiler;

////////////////////////////////////////////////////////////////////////////////

static NConcurrency::TFls<bool> HiveMutation;

bool IsHiveMutation()
{
    return *HiveMutation;
}

class THiveMutationGuard
    : private TNonCopyable
{
public:
    THiveMutationGuard()
    {
        YT_ASSERT(!*HiveMutation);
        *HiveMutation = true;
    }

    ~THiveMutationGuard()
    {
        *HiveMutation = false;
    }
};

////////////////////////////////////////////////////////////////////////////////

class THiveManager::TImpl
    : public THydraServiceBase
    , public TCompositeAutomatonPart
{
public:
    TImpl(
        THiveManagerConfigPtr config,
        TCellDirectoryPtr cellDirectory,
        TCellId selfCellId,
        IInvokerPtr automatonInvoker,
        IHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr automaton)
        : THydraServiceBase(
            hydraManager->CreateGuardedAutomatonInvoker(automatonInvoker),
            THiveServiceProxy::GetDescriptor(),
            HiveServerLogger,
            selfCellId)
        , TCompositeAutomatonPart(
            hydraManager,
            automaton,
            automatonInvoker)
        , SelfCellId_(selfCellId)
        , Config_(std::move(config))
        , CellDirectory_(std::move(cellDirectory))
        , AutomatonInvoker_(std::move(automatonInvoker))
        , GuardedAutomatonInvoker_(hydraManager->CreateGuardedAutomatonInvoker(AutomatonInvoker_))
        , HydraManager_(std::move(hydraManager))
    {
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(Ping));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(SyncCells));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(PostMessages));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(SendMessages));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(SyncWithOthers));

        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraAcknowledgeMessages, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraPostMessages, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraSendMessages, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraRegisterMailbox, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraUnregisterMailbox, Unretained(this)));

        RegisterLoader(
            "HiveManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "HiveManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "HiveManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "HiveManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));

        OrchidService_ = CreateOrchidService();
    }

    IServicePtr GetRpcService()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return this;
    }

    IYPathServicePtr GetOrchidService()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return OrchidService_;
    }

    TCellId GetSelfCellId() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return SelfCellId_;
    }

    TMailbox* CreateMailbox(TCellId cellId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (RemovedCellIds_.erase(cellId) != 0) {
            YT_LOG_ALERT_UNLESS(IsRecovery(), "Mailbox has been resurrected (SelfCellId: %v, CellId: %v)",
                SelfCellId_,
                cellId);
        }

        auto mailboxHolder = std::make_unique<TMailbox>(cellId);
        auto* mailbox = MailboxMap_.Insert(cellId, std::move(mailboxHolder));

        if (!IsRecovery()) {
            SendPeriodicPing(mailbox);
        }

        YT_LOG_INFO_UNLESS(IsRecovery(), "Mailbox created (SelfCellId: %v, CellId: %v)",
            SelfCellId_,
            mailbox->GetCellId());
        return mailbox;
    }

    TMailbox* FindMailbox(TCellId cellId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return MailboxMap_.Find(cellId);
    }

    TMailbox* GetOrCreateMailbox(TCellId cellId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* mailbox = MailboxMap_.Find(cellId);
        if (!mailbox) {
            mailbox = CreateMailbox(cellId);
        }
        return mailbox;
    }

    TMailbox* GetMailboxOrThrow(TCellId cellId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* mailbox = FindMailbox(cellId);
        if (!mailbox) {
            THROW_ERROR_EXCEPTION("No such mailbox %v",
                cellId);
        }
        return mailbox;
    }

    void RemoveMailbox(TMailbox* mailbox)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto cellId = mailbox->GetCellId();

        MailboxMap_.Remove(cellId);

        if (!RemovedCellIds_.insert(cellId).second) {
            YT_LOG_ALERT_UNLESS(IsRecovery(), "Mailbox is already removed (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                cellId);
        }

        YT_LOG_INFO_UNLESS(IsRecovery(), "Mailbox removed (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            cellId);
    }

    void PostMessage(TMailbox* mailbox, const TSerializedMessagePtr& message, bool reliable)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        PostMessage(TMailboxList{mailbox}, message, reliable);
    }

    void PostMessage(const TMailboxList& mailboxes, const TSerializedMessagePtr& message, bool reliable)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (reliable) {
            ReliablePostMessage(mailboxes, message);
        } else {
            UnreliablePostMessage(mailboxes, message);
        }
    }

    void PostMessage(TMailbox* mailbox, const ::google::protobuf::MessageLite& message, bool reliable)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        PostMessage(mailbox, SerializeOutcomingMessage(message), reliable);
    }

    void PostMessage(const TMailboxList& mailboxes, const ::google::protobuf::MessageLite& message, bool reliable)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        PostMessage(mailboxes, SerializeOutcomingMessage(message), reliable);
    }

    TFuture<void> SyncWith(TCellId cellId, bool enableBatching)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (cellId == SelfCellId_) {
            return VoidFuture;
        }

        if (enableBatching) {
            return GetOrCreateSyncBatcher(cellId)->Run();
        } else {
            return DoSyncWithCore(cellId).ToImmediatelyCancelable();
        }
    }

    DECLARE_ENTITY_MAP_ACCESSORS(Mailbox, TMailbox)

private:
    const TCellId SelfCellId_;
    const THiveManagerConfigPtr Config_;
    const TCellDirectoryPtr CellDirectory_;
    const IInvokerPtr AutomatonInvoker_;
    const IInvokerPtr GuardedAutomatonInvoker_;
    const IHydraManagerPtr HydraManager_;

    IYPathServicePtr OrchidService_;

    TEntityMap<TMailbox> MailboxMap_;
    THashMap<TCellId, TMessageId> CellIdToNextTransientIncomingMessageId_;

    THashSet<TCellId> RemovedCellIds_;

    TReaderWriterSpinLock CellToIdToBatcherLock_;
    THashMap<TCellId, TIntrusivePtr<TAsyncBatcher<void>>> CellToIdToBatcher_;

    TMonotonicCounter PostingTimeCounter_{"/posting_time"};

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    // RPC handlers.

    DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto, Ping)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto srcCellId = FromProto<TCellId>(request->src_cell_id());

        context->SetRequestInfo("SrcCellId: %v, DstCellId: %v",
            srcCellId,
            SelfCellId_);

        ValidatePeer(EPeerKind::Leader);

        auto* mailbox = FindMailbox(srcCellId);
        auto lastOutcomingMessageId = mailbox
            ? std::make_optional(mailbox->GetFirstOutcomingMessageId() + static_cast<int>(mailbox->OutcomingMessages().size()) - 1)
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

        auto knownCells = FromProto<std::vector<TCellInfo>>(request->known_cells());
        auto syncResult = CellDirectory_->Synchronize(knownCells);

        for (const auto& request : syncResult.ReconfigureRequests) {
            YT_LOG_DEBUG("Requesting cell reconfiguration (CellId: %v, ConfigVersion: %v -> %v)",
                request.NewDescriptor.CellId,
                request.OldConfigVersion,
                request.NewDescriptor.ConfigVersion);
            auto* protoInfo = response->add_cells_to_reconfigure();
            ToProto(protoInfo->mutable_cell_descriptor(), request.NewDescriptor);
        }

        for (const auto& request : syncResult.UnregisterRequests) {
            YT_LOG_DEBUG("Requesting cell unregistration (CellId: %v)",
                request.CellId);
            auto* unregisterInfo = response->add_cells_to_unregister();
            ToProto(unregisterInfo->mutable_cell_id(), request.CellId);
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto, PostMessages)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto srcCellId = FromProto<TCellId>(request->src_cell_id());
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
            CreateMutation(HydraManager_, hydraRequest)
                ->CommitAndLog(Logger);

            THROW_ERROR_EXCEPTION(
                NHiveClient::EErrorCode::MailboxNotCreatedYet,
                "Mailbox %v is not created yet",
                srcCellId);
        }

        auto nextTransientIncomingMessageId = mailbox->GetNextTransientIncomingMessageId();
        YT_VERIFY(nextTransientIncomingMessageId >= 0);
        if (nextTransientIncomingMessageId == firstMessageId && messageCount > 0) {
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Committing reliable incoming messages (SrcCellId: %v, DstCellId: %v, "
                "MessageIds: %v-%v)",
                srcCellId,
                SelfCellId_,
                firstMessageId,
                firstMessageId + messageCount - 1);

            mailbox->SetNextTransientIncomingMessageId(nextTransientIncomingMessageId + messageCount);
            CreatePostMessagesMutation(*request)
                ->CommitAndLog(Logger);
        }
        response->set_next_transient_incoming_message_id(nextTransientIncomingMessageId);

        auto nextPersistentIncomingMessageId = mailbox->GetNextPersistentIncomingMessageId();
        response->set_next_persistent_incoming_message_id(nextPersistentIncomingMessageId);

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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Committing unreliable incoming messages (SrcCellId: %v, DstCellId: %v, "
            "MessageCount: %v)",
            srcCellId,
            SelfCellId_,
            messageCount);

        CreateSendMessagesMutation(context)
            ->CommitAndReply(context);
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

        context->ReplyFrom(Combine(asyncResults));
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
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "No messages acknowledged (SrcCellId: %v, DstCellId: %v, "
                "NextPersistentIncomingMessageId: %v, FirstOutcomingMessageId: %v)",
                SelfCellId_,
                mailbox->GetCellId(),
                nextPersistentIncomingMessageId,
                mailbox->GetFirstOutcomingMessageId());
            return;
        }

        auto& outcomingMessages = mailbox->OutcomingMessages();
        if (acknowledgeCount > outcomingMessages.size()) {
            YT_LOG_ALERT_UNLESS(IsRecovery(), "Requested to acknowledge too many messages (SrcCellId: %v, DstCellId: %v, "
                "NextPersistentIncomingMessageId: %v, FirstOutcomingMessageId: %v, OutcomingMessageCount: %v)",
                SelfCellId_,
                mailbox->GetCellId(),
                nextPersistentIncomingMessageId,
                mailbox->GetFirstOutcomingMessageId(),
                outcomingMessages.size());
            return;
        }

        outcomingMessages.erase(outcomingMessages.begin(), outcomingMessages.begin() + acknowledgeCount);
        mailbox->SetFirstOutcomingMessageId(mailbox->GetFirstOutcomingMessageId() + acknowledgeCount);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Messages acknowledged (SrcCellId: %v, DstCellId: %v, "
            "FirstOutcomingMessageId: %v)",
            SelfCellId_,
            mailbox->GetCellId(),
            mailbox->GetFirstOutcomingMessageId());
    }

    void HydraPostMessages(NHiveClient::NProto::TReqPostMessages* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto srcCellId = FromProto<TCellId>(request->src_cell_id());

        ValidateCellNotRemoved(srcCellId);

        auto firstMessageId = request->first_message_id();
        auto* mailbox = FindMailbox(srcCellId);
        if (!mailbox) {
            if (firstMessageId != 0) {
                YT_LOG_ALERT_UNLESS(IsRecovery(), "Received a non-initial message to a missing mailbox (SrcCellId: %v, MessageId: %v)",
                    srcCellId,
                    firstMessageId);
                return;
            }
            mailbox = CreateMailbox(srcCellId);
        }

        ApplyReliableIncomingMessages(mailbox, request);
    }

    void HydraSendMessages(
        const TCtxSendMessagesPtr& /*context*/,
        NHiveClient::NProto::TReqSendMessages* request,
        NHiveClient::NProto::TRspSendMessages* /*response*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto srcCellId = FromProto<TCellId>(request->src_cell_id());
        auto* mailbox = GetMailboxOrThrow(srcCellId);
        ApplyUnreliableIncomingMessages(mailbox, request);
    }

    void HydraRegisterMailbox(NHiveServer::NProto::TReqRegisterMailbox* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto cellId = FromProto<TCellId>(request->cell_id());
        if (RemovedCellIds_.contains(cellId)) {
            YT_LOG_INFO_UNLESS(IsRecovery(), "Mailbox is already removed (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                cellId);
            return;
        }

        GetOrCreateMailbox(cellId);
    }

    void HydraUnregisterMailbox(NHiveServer::NProto::TReqUnregisterMailbox* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto cellId = FromProto<TCellId>(request->cell_id());
        if (auto* mailbox = FindMailbox(cellId)) {
            RemoveMailbox(mailbox);
        }
    }


    NRpc::IChannelPtr FindMailboxChannel(TMailbox* mailbox)
    {
        auto now = GetCpuInstant();
        auto cachedChannel = mailbox->GetCachedChannel();
        if (cachedChannel && now < mailbox->GetCachedChannelDeadline()) {
            return cachedChannel;
        }

        auto channel = CellDirectory_->FindChannel(mailbox->GetCellId());
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
        YT_VERIFY(HasMutationContext());

        TStringBuilder logMessageBuilder;
        logMessageBuilder.AppendFormat("Reliable outcoming message added (MutationType: %v, SrcCellId: %v, DstCellIds: {",
            message->Type,
            SelfCellId_);

        auto traceContext = NTracing::GetCurrentTraceContext();

        for (auto* mailbox : mailboxes) {
            auto messageId =
                mailbox->GetFirstOutcomingMessageId() +
                mailbox->OutcomingMessages().size();

            mailbox->OutcomingMessages().push_back({
                message,
                traceContext
            });

            if (mailbox != mailboxes.front()) {
                logMessageBuilder.AppendString(AsStringBuf(", "));
            }
            logMessageBuilder.AppendFormat("%v=>%v",
                mailbox->GetCellId(),
                messageId);

            SchedulePostOutcomingMessages(mailbox);
        }

        logMessageBuilder.AppendString(AsStringBuf("})"));
        YT_LOG_DEBUG_UNLESS(IsRecovery(), logMessageBuilder.Flush());
    }

    void UnreliablePostMessage(const TMailboxList& mailboxes, const TSerializedMessagePtr& message)
    {
        TCounterIncrementingTimingGuard<TWallTimer> timingGuard(Profiler, &PostingTimeCounter_);

        TStringBuilder logMessageBuilder;
        logMessageBuilder.AppendFormat("Sending unreliable outcoming message (MutationType: %v, SrcCellId: %v, DstCellIds: [",
            message->Type,
            SelfCellId_);

        auto traceContext = NTracing::GetCurrentTraceContext();

        for (auto* mailbox : mailboxes) {
            if (!mailbox->GetConnected()) {
                continue;
            }

            auto channel = FindMailboxChannel(mailbox);
            if (!channel) {
                continue;
            }

            if (mailbox != mailboxes.front()) {
                logMessageBuilder.AppendString(AsStringBuf(", "));
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
                BIND(&TImpl::OnSendMessagesResponse, MakeStrong(this), mailbox->GetCellId())
                    .Via(EpochAutomatonInvoker_));
        }

        logMessageBuilder.AppendString(AsStringBuf("])"));
        YT_LOG_DEBUG_UNLESS(IsRecovery(), logMessageBuilder.Flush());
    }


    void SetMailboxConnected(TMailbox* mailbox)
    {
        if (mailbox->GetConnected()) {
            return;
        }

        mailbox->SetConnected(true);
        YT_VERIFY(mailbox->SyncRequests().empty());
        mailbox->SetFirstInFlightOutcomingMessageId(mailbox->GetFirstOutcomingMessageId());
        YT_VERIFY(mailbox->GetInFlightOutcomingMessageCount() == 0);

        YT_LOG_INFO("Mailbox connected (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            mailbox->GetCellId());

        PostOutcomingMessages(mailbox, true);
    }

    void SetMailboxDisconnected(TMailbox* mailbox)
    {
        if (!mailbox->GetConnected()) {
            return;
        }

        mailbox->SetConnected(false);
        mailbox->SetPostInProgress(false);
        mailbox->SyncRequests().clear();
        mailbox->SetFirstInFlightOutcomingMessageId(mailbox->GetFirstOutcomingMessageId());
        mailbox->SetInFlightOutcomingMessageCount(0);
        TDelayedExecutor::CancelAndClear(mailbox->IdlePostCookie());

        YT_LOG_INFO("Mailbox disconnected (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            mailbox->GetCellId());
    }

    void ResetMailboxes()
    {
        decltype(CellToIdToBatcher_) cellToIdToBatcher;
        {
            TWriterGuard guard(CellToIdToBatcherLock_);
            std::swap(cellToIdToBatcher, CellToIdToBatcher_);
        }

        auto error = TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped");
        for (const auto& [cellId, batcher] : cellToIdToBatcher) {
            batcher->Cancel(error);
        }

        for (auto [id, mailbox] : MailboxMap_) {
            SetMailboxDisconnected(mailbox);
            mailbox->SetNextTransientIncomingMessageId(-1);
            mailbox->SetAcknowledgeInProgress(false);
            mailbox->SetCachedChannel(nullptr);
            mailbox->SetPostBatchingCookie(nullptr);
        }
    }

    void PrepareLeaderMailboxes()
    {
        for (auto [id, mailbox] : MailboxMap_) {
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


    void SchedulePeriodicPing(TMailbox* mailbox)
    {
        TDelayedExecutor::Submit(
            BIND(&TImpl::OnPeriodicPingTick, MakeWeak(this), mailbox->GetCellId())
                .Via(EpochAutomatonInvoker_),
            Config_->PingPeriod);
    }

    void ReconnectMailboxes()
    {
        for (const auto& pair : MailboxMap_) {
            auto* mailbox = pair.second;
            YT_VERIFY(!mailbox->GetConnected());
            SendPeriodicPing(mailbox);
        }
    }

    void OnPeriodicPingTick(TCellId cellId)
    {
        auto* mailbox = FindMailbox(cellId);
        if (!mailbox) {
            return;
        }

        SendPeriodicPing(mailbox);
    }

    void SendPeriodicPing(TMailbox* mailbox)
    {
        auto cellId = mailbox->GetCellId();

        if (IsLeader() && CellDirectory_->IsCellUnregistered(cellId)) {
            NHiveServer::NProto::TReqUnregisterMailbox req;
            ToProto(req.mutable_cell_id(), cellId);
            CreateUnregisterMailboxMutation(req)
                ->CommitAndLog(Logger);
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

        THiveServiceProxy proxy(std::move(channel));

        auto req = proxy.Ping();
        req->SetTimeout(Config_->PingRpcTimeout);
        ToProto(req->mutable_src_cell_id(), SelfCellId_);

        req->Invoke().Subscribe(
            BIND(&TImpl::OnPeriodicPingResponse, MakeStrong(this), mailbox->GetCellId())
                .Via(EpochAutomatonInvoker_));
    }

    void OnPeriodicPingResponse(TCellId cellId, const THiveServiceProxy::TErrorOrRspPingPtr& rspOrError)
    {
        auto* mailbox = FindMailbox(cellId);
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
        // COMPAT(babenko): last_outcoming_message_id is now required
        auto lastOutcomingMessageId = rsp->has_last_outcoming_message_id()
            ? std::make_optional(rsp->last_outcoming_message_id())
            : std::nullopt;

        YT_LOG_DEBUG("Periodic ping succeeded (SrcCellId: %v, DstCellId: %v, LastOutcomingMessageId: %v)",
            SelfCellId_,
            mailbox->GetCellId(),
            lastOutcomingMessageId);

        SetMailboxConnected(mailbox);
    }


    TIntrusivePtr<TAsyncBatcher<void>> GetOrCreateSyncBatcher(TCellId cellId)
    {
        {
            TReaderGuard readerGuard(CellToIdToBatcherLock_);
            auto it = CellToIdToBatcher_.find(cellId);
            if (it != CellToIdToBatcher_.end()) {
                return it->second;
            }
        }

        auto batcher = New<TAsyncBatcher<void>>(
            BIND_DONT_CAPTURE_TRACE_CONTEXT(&TImpl::DoSyncWith, MakeWeak(this), cellId),
            Config_->SyncDelay);

        {
            TWriterGuard writerGuard(CellToIdToBatcherLock_);
            auto it = CellToIdToBatcher_.emplace(cellId, std::move(batcher)).first;
            return it->second;
        }
    }

    static TFuture<void> DoSyncWith(const TWeakPtr<TImpl>& weakThis, TCellId cellId)
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

        auto channel = CellDirectory_->FindChannel(cellId, EPeerKind::Leader);
        if (!channel) {
            return MakeFuture(TError(
                NRpc::EErrorCode::Unavailable,
                "Cannot synchronize with cell %v since it is not connected",
                cellId));
        }

        YT_LOG_DEBUG("Synchronizing with another instance (SrcCellId: %v, DstCellId: %v)",
            cellId,
            SelfCellId_);

        THiveServiceProxy proxy(std::move(channel));

        auto req = proxy.Ping();
        req->SetTimeout(Config_->PingRpcTimeout);
        ToProto(req->mutable_src_cell_id(), SelfCellId_);

        return req->Invoke()
            .Apply(
                BIND(&TImpl::OnSyncPingResponse, MakeStrong(this), cellId)
                    .AsyncVia(GuardedAutomatonInvoker_))
            .WithTimeout(Config_->SyncTimeout)
            // NB: Many subscribers are typically waiting for the sync to complete.
            // Make sure the promise is set in a large thread pool.
            .Apply(
                 BIND([] (const TError& error) { error.ThrowOnError(); })
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

        auto* mailbox = GetMailboxOrThrow(cellId);
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

    TFuture<void> RegisterSyncRequest(TMailbox* mailbox, TMessageId messageId)
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

    void FlushSyncRequests(TMailbox* mailbox)
    {
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
        TCounterIncrementingTimingGuard<TWallTimer> timingGuard(Profiler, &PostingTimeCounter_);

        auto* mailbox = FindMailbox(cellId);
        if (!mailbox) {
            return;
        }

        PostOutcomingMessages(mailbox, true);
    }

    void SchedulePostOutcomingMessages(TMailbox* mailbox)
    {
        if (mailbox->GetPostBatchingCookie()) {
            return;
        }

        mailbox->SetPostBatchingCookie(TDelayedExecutor::Submit(
            BIND_DONT_CAPTURE_TRACE_CONTEXT([this, this_ = MakeStrong(this), cellId = mailbox->GetCellId()] {
                TCounterIncrementingTimingGuard<TWallTimer> timingGuard(Profiler, &PostingTimeCounter_);

                auto* mailbox = FindMailbox(cellId);
                if (!mailbox) {
                    return;
                }

                mailbox->SetPostBatchingCookie(nullptr);
                PostOutcomingMessages(mailbox, false);
            }).Via(EpochAutomatonInvoker_),
            Config_->PostBatchingPeriod));
    }

    void PostOutcomingMessages(TMailbox* mailbox, bool allowIdle)
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

        auto firstMessageId = mailbox->GetFirstInFlightOutcomingMessageId();
        const auto& outcomingMessages = mailbox->OutcomingMessages();
        YT_VERIFY(firstMessageId >= mailbox->GetFirstOutcomingMessageId());
        YT_VERIFY(firstMessageId <= mailbox->GetFirstOutcomingMessageId() + outcomingMessages.size());

        TDelayedExecutor::CancelAndClear(mailbox->IdlePostCookie());
        if (!allowIdle && firstMessageId == mailbox->GetFirstOutcomingMessageId() + outcomingMessages.size()) {
            mailbox->IdlePostCookie() = TDelayedExecutor::Submit(
                BIND_DONT_CAPTURE_TRACE_CONTEXT(&TImpl::OnIdlePostOutcomingMessages, MakeWeak(this), mailbox->GetCellId())
                    .Via(EpochAutomatonInvoker_),
                Config_->IdlePostPeriod);
            return;
        }

        auto channel = FindMailboxChannel(mailbox);
        if (!channel) {
            return;
        }

        THiveServiceProxy proxy(std::move(channel));

        auto req = proxy.PostMessages();
        req->SetTimeout(Config_->PostRpcTimeout);
        ToProto(req->mutable_src_cell_id(), SelfCellId_);
        req->set_first_message_id(firstMessageId);

        int messagesToPost = 0;
        i64 bytesToPost = 0;
        while (firstMessageId + messagesToPost < mailbox->GetFirstOutcomingMessageId() + outcomingMessages.size() &&
               messagesToPost < Config_->MaxMessagesPerPost &&
               bytesToPost < Config_->MaxBytesPerPost)
        {
            const auto& message = outcomingMessages[firstMessageId + messagesToPost - mailbox->GetFirstOutcomingMessageId()];
            auto* protoMessage = req->add_messages();
            protoMessage->set_type(message.SerializedMessage->Type);
            protoMessage->set_data(message.SerializedMessage->Data);
            if (message.TraceContext) {
                ToProto(protoMessage->mutable_tracing_ext(), message.TraceContext);
            }
            messagesToPost += 1;
            bytesToPost += message.SerializedMessage->Data.size();
        }

        mailbox->SetInFlightOutcomingMessageCount(messagesToPost);
        mailbox->SetPostInProgress(true);

        if (messagesToPost == 0) {
            YT_LOG_DEBUG("Checking mailbox synchronization (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                mailbox->GetCellId());
        } else {
            YT_LOG_DEBUG("Posting reliable outcoming messages (SrcCellId: %v, DstCellId: %v, MessageIds: %v-%v)",
                SelfCellId_,
                mailbox->GetCellId(),
                firstMessageId,
                firstMessageId + messagesToPost - 1);
        }

        req->Invoke().Subscribe(
            BIND(&TImpl::OnPostMessagesResponse, MakeStrong(this), mailbox->GetCellId())
                .Via(EpochAutomatonInvoker_));
    }

    void OnPostMessagesResponse(TCellId cellId, const THiveServiceProxy::TErrorOrRspPostMessagesPtr& rspOrError)
    {
        TCounterIncrementingTimingGuard<TWallTimer> timingGuard(Profiler, &PostingTimeCounter_);

        auto* mailbox = FindMailbox(cellId);
        if (!mailbox) {
            return;
        }

        if (!mailbox->GetPostInProgress()) {
            return;
        }

        mailbox->SetInFlightOutcomingMessageCount(0);
        mailbox->SetPostInProgress(false);

        if (rspOrError.GetCode() == NHiveClient::EErrorCode::MailboxNotCreatedYet) {
            YT_LOG_DEBUG(rspOrError, "Mailbox is not created yet; will retry (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                mailbox->GetCellId());
            SchedulePostOutcomingMessages(mailbox);
            return;
        }

        if (!rspOrError.IsOK()) {
            YT_LOG_DEBUG(rspOrError, "Failed to post reliable outcoming messages (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                mailbox->GetCellId());
            SetMailboxDisconnected(mailbox);
            return;
        }

        const auto& rsp = rspOrError.Value();
        // COMPAT(babenko): next_persistent_incoming_message_id is now required
        auto nextPersistentIncomingMessageId = rsp->has_next_persistent_incoming_message_id()
            ? std::make_optional(rsp->next_persistent_incoming_message_id())
            : std::nullopt;
        auto nextTransientIncomingMessageId = rsp->next_transient_incoming_message_id();
        YT_LOG_DEBUG("Outcoming reliable messages posted (SrcCellId: %v, DstCellId: %v, "
            "NextPersistentIncomingMessageId: %v, NextTransientIncomingMessageId: %v)",
            SelfCellId_,
            mailbox->GetCellId(),
            nextPersistentIncomingMessageId,
            nextTransientIncomingMessageId);

        if (nextPersistentIncomingMessageId && !HandlePersistentIncomingMessages(mailbox, *nextPersistentIncomingMessageId)) {
            return;
        }

        if (!HandleTransientIncomingMessages(mailbox, nextTransientIncomingMessageId)) {
            return;
        }

        SchedulePostOutcomingMessages(mailbox);
    }

    void OnSendMessagesResponse(TCellId cellId, const THiveServiceProxy::TErrorOrRspSendMessagesPtr& rspOrError)
    {
        TCounterIncrementingTimingGuard<TWallTimer> timingGuard(Profiler, &PostingTimeCounter_);

        auto* mailbox = FindMailbox(cellId);
        if (!mailbox) {
            return;
        }

        if (!rspOrError.IsOK()) {
            YT_LOG_DEBUG(rspOrError, "Failed to send unreliable outcoming messages (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                mailbox->GetCellId());
            SetMailboxDisconnected(mailbox);
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
            &TImpl::HydraAcknowledgeMessages,
            this);
    }

    std::unique_ptr<TMutation> CreatePostMessagesMutation(const NHiveClient::NProto::TReqPostMessages& request)
    {
        return CreateMutation(
            HydraManager_,
            request,
            &TImpl::HydraPostMessages,
            this);
    }

    std::unique_ptr<TMutation> CreateSendMessagesMutation(const TCtxSendMessagesPtr& context)
    {
        return CreateMutation(
            HydraManager_,
            context,
            &TImpl::HydraSendMessages,
            this);
    }

    std::unique_ptr<TMutation> CreateRegisterMailboxMutation(const NHiveServer::NProto::TReqRegisterMailbox& req)
    {
        return CreateMutation(
            HydraManager_,
            req,
            &TImpl::HydraRegisterMailbox,
            this);
    }

    std::unique_ptr<TMutation> CreateUnregisterMailboxMutation(const NHiveServer::NProto::TReqUnregisterMailbox& req)
    {
        return CreateMutation(
            HydraManager_,
            req,
            &TImpl::HydraUnregisterMailbox,
            this);
    }


    bool CheckRequestedMessageIdAgainstMailbox(TMailbox* mailbox, TMessageId requestedMessageId)
    {
        if (requestedMessageId < mailbox->GetFirstOutcomingMessageId()) {
            YT_LOG_ALERT_UNLESS(IsRecovery(), "Destination is out of sync: requested to receive already truncated messages (SrcCellId: %v, DstCellId: %v, "
                "RequestedMessageId: %v, FirstOutcomingMessageId: %v)",
                SelfCellId_,
                mailbox->GetCellId(),
                requestedMessageId,
                mailbox->GetFirstOutcomingMessageId());
            SetMailboxDisconnected(mailbox);
            return false;
        }

        if (requestedMessageId > mailbox->GetFirstOutcomingMessageId() + mailbox->OutcomingMessages().size()) {
            YT_LOG_ALERT_UNLESS(IsRecovery(), "Destination is out of sync: requested to receive nonexisting messages (SrcCellId: %v, DstCellId: %v, "
                "RequestedMessageId: %v, FirstOutcomingMessageId: %v, OutcomingMessageCount: %v)",
                SelfCellId_,
                mailbox->GetCellId(),
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
        ToProto(req.mutable_cell_id(), mailbox->GetCellId());
        req.set_next_persistent_incoming_message_id(nextPersistentIncomingMessageId);

        mailbox->SetAcknowledgeInProgress(true);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Committing reliable messages acknowledgement (SrcCellId: %v, DstCellId: %v, "
            "MessageIds: %v-%v)",
            SelfCellId_,
            mailbox->GetCellId(),
            mailbox->GetFirstOutcomingMessageId(),
            nextPersistentIncomingMessageId - 1);

        CreateAcknowledgeMessagesMutation(req)
            ->CommitAndLog(Logger);

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
            YT_LOG_ALERT_UNLESS(IsRecovery(), "Attempt to apply an out-of-order message (SrcCellId: %v, DstCellId: %v, "
                "ExpectedMessageId: %v, ActualMessageId: %v, MutationType: %v)",
                mailbox->GetCellId(),
                SelfCellId_,
                mailbox->GetNextPersistentIncomingMessageId(),
                messageId,
                message.type());
            return;
        }

        auto traceContext = message.has_tracing_ext()
            ? NTracing::CreateChildTraceContext(
                message.tracing_ext(),
                ConcatToString(AsStringBuf("HiveManager:"), message.type()))
            : nullptr;
        TTraceContextGuard traceContextGuard(std::move(traceContext));

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Applying reliable incoming message (SrcCellId: %v, DstCellId: %v, MessageId: %v, MutationType: %v)",
            mailbox->GetCellId(),
            SelfCellId_,
            messageId,
            message.type());

        ApplyMessage(message);

        mailbox->SetNextPersistentIncomingMessageId(messageId + 1);

        FlushSyncRequests(mailbox);
    }

    void ApplyUnreliableIncomingMessages(TMailbox* mailbox, const NHiveClient::NProto::TReqSendMessages* req)
    {
        for (const auto& message : req->messages()) {
            ApplyUnreliableIncomingMessage(mailbox, message);
        }
    }

    void ApplyUnreliableIncomingMessage(TMailbox* mailbox, const TEncapsulatedMessage& message)
    {
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Applying unreliable incoming message (SrcCellId: %v, DstCellId: %v, MutationType: %v)",
            mailbox->GetCellId(),
            SelfCellId_,
            message.type());
        ApplyMessage(message);
    }

    void ApplyMessage(const TEncapsulatedMessage& message)
    {
        TMutationRequest request;
        request.Reign = GetCurrentMutationContext()->Request().Reign;
        request.Type = message.type();
        request.Data = TSharedRef::FromString(message.data());

        TMutationContext mutationContext(GetCurrentMutationContext(), request);
        TMutationContextGuard mutationContextGuard(&mutationContext);

        THiveMutationGuard hiveMutationGuard;

        static_cast<IAutomaton*>(Automaton_)->ApplyMutation(&mutationContext);
    }


    // NB: Leader must wait until it is active before reconnecting mailboxes
    // since no commits are possible before this point.
    virtual void OnLeaderActive() override
    {
        TCompositeAutomatonPart::OnLeaderRecoveryComplete();
        ReconnectMailboxes();
        PrepareLeaderMailboxes();
    }

    virtual void OnStopLeading() override
    {
        TCompositeAutomatonPart::OnStopLeading();
        ResetMailboxes();
    }

    virtual void OnFollowerRecoveryComplete() override
    {
        TCompositeAutomatonPart::OnFollowerRecoveryComplete();
        ReconnectMailboxes();
    }

    virtual void OnStopFollowing() override
    {
        TCompositeAutomatonPart::OnStopFollowing();
        ResetMailboxes();
    }


    virtual bool ValidateSnapshotVersion(int version) override
    {
        return
            version == 3 ||
            version == 4 ||
            version == 5;
    }

    virtual int GetCurrentSnapshotVersion() override
    {
        return 5;
    }


    virtual void Clear() override
    {
        TCompositeAutomatonPart::Clear();

        MailboxMap_.Clear();
    }

    void SaveKeys(TSaveContext& context) const
    {
        MailboxMap_.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context) const
    {
        MailboxMap_.SaveValues(context);
        Save(context, RemovedCellIds_);
    }

    void LoadKeys(TLoadContext& context)
    {
        MailboxMap_.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        MailboxMap_.LoadValues(context);
        // COMPAT(babenko)
        if (context.GetVersion() >= 4) {
            Load(context, RemovedCellIds_);
        }
    }


    // THydraServiceBase overrides.
    virtual IHydraManagerPtr GetHydraManager() override
    {
        return HydraManager_;
    }


    IYPathServicePtr CreateOrchidService()
    {
        auto invoker = HydraManager_->CreateGuardedAutomatonInvoker(AutomatonInvoker_);
        auto producer = BIND(&TImpl::BuildOrchidYson, MakeWeak(this));
        return IYPathService::FromProducer(producer, TDuration::Seconds(1))
            ->Via(invoker);
    }

    void BuildOrchidYson(IYsonConsumer* consumer)
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("mailboxes").DoMapFor(MailboxMap_, [&] (TFluentMap fluent, const std::pair<TCellId, TMailbox*>& pair) {
                    auto* mailbox = pair.second;
                    fluent
                        .Item(ToString(mailbox->GetCellId())).BeginMap()
                            .Item("connected").Value(mailbox->GetConnected())
                            .Item("acknowledge_in_progress").Value(mailbox->GetAcknowledgeInProgress())
                            .Item("post_in_progress").Value(mailbox->GetPostInProgress())
                            .Item("first_outcoming_message_id").Value(mailbox->GetFirstOutcomingMessageId())
                            .Item("outcoming_message_count").Value(mailbox->OutcomingMessages().size())
                            .Item("next_persistent_incoming_message_id").Value(mailbox->GetNextPersistentIncomingMessageId())
                            .Item("next_transient_incoming_message_id").Value(mailbox->GetNextTransientIncomingMessageId())
                            .Item("first_in_flight_outcoming_message_id").Value(mailbox->GetFirstInFlightOutcomingMessageId())
                            .Item("in_flight_outcoming_message_count").Value(mailbox->GetInFlightOutcomingMessageCount())
                        .EndMap();
                })
            .EndMap();
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(THiveManager::TImpl, Mailbox, TMailbox, MailboxMap_)

////////////////////////////////////////////////////////////////////////////////

THiveManager::THiveManager(
    THiveManagerConfigPtr config,
    TCellDirectoryPtr cellDirectory,
    TCellId selfCellId,
    IInvokerPtr automatonInvoker,
    IHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton)
    : Impl_(New<TImpl>(
        config,
        cellDirectory,
        selfCellId,
        automatonInvoker,
        hydraManager,
        automaton))
{ }

THiveManager::~THiveManager()
{ }

IServicePtr THiveManager::GetRpcService()
{
    return Impl_->GetRpcService();
}

IYPathServicePtr THiveManager::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

TCellId THiveManager::GetSelfCellId() const
{
    return Impl_->GetSelfCellId();
}

TMailbox* THiveManager::CreateMailbox(TCellId cellId)
{
    return Impl_->CreateMailbox(cellId);
}

TMailbox* THiveManager::FindMailbox(TCellId cellId)
{
    return Impl_->FindMailbox(cellId);
}

TMailbox* THiveManager::GetOrCreateMailbox(TCellId cellId)
{
    return Impl_->GetOrCreateMailbox(cellId);
}

TMailbox* THiveManager::GetMailboxOrThrow(TCellId cellId)
{
    return Impl_->GetMailboxOrThrow(cellId);
}

void THiveManager::RemoveMailbox(TMailbox* mailbox)
{
    Impl_->RemoveMailbox(mailbox);
}

void THiveManager::PostMessage(TMailbox* mailbox, const TSerializedMessagePtr& message, bool reliable)
{
    Impl_->PostMessage(mailbox, message, reliable);
}

void THiveManager::PostMessage(const TMailboxList& mailboxes, const TSerializedMessagePtr& message, bool reliable)
{
    Impl_->PostMessage(mailboxes, message, reliable);
}

void THiveManager::PostMessage(TMailbox* mailbox, const ::google::protobuf::MessageLite& message, bool reliable)
{
    Impl_->PostMessage(mailbox, message, reliable);
}

void THiveManager::PostMessage(const TMailboxList& mailboxes, const ::google::protobuf::MessageLite& message, bool reliable)
{
    Impl_->PostMessage(mailboxes, message, reliable);
}

TFuture<void> THiveManager::SyncWith(TCellId cellId, bool enableBatching)
{
    return Impl_->SyncWith(cellId, enableBatching);
}

DELEGATE_ENTITY_MAP_ACCESSORS(THiveManager, Mailbox, TMailbox, *Impl_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
