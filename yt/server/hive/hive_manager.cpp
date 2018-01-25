#include "hive_manager.h"
#include "config.h"
#include "mailbox.h"
#include "helpers.h"
#include "private.h"

#include <yt/server/election/election_manager.h>

#include <yt/server/hydra/composite_automaton.h>
#include <yt/server/hydra/hydra_manager.h>
#include <yt/server/hydra/hydra_service.h>
#include <yt/server/hydra/mutation.h>
#include <yt/server/hydra/mutation_context.h>

#include <yt/ytlib/hive/cell_directory.h>
#include <yt/ytlib/hive/hive_service_proxy.h>

#include <yt/ytlib/hydra/config.h>
#include <yt/ytlib/hydra/peer_channel.h>

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/fls.h>

#include <yt/core/net/local_address.h>

#include <yt/core/rpc/proto/rpc.pb.h>
#include <yt/core/rpc/server.h>
#include <yt/core/rpc/service_detail.h>

#include <yt/core/tracing/trace_context.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NHiveServer {

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

using NYT::ToProto;
using NYT::FromProto;

using NHiveClient::NProto::TEncapsulatedMessage;

////////////////////////////////////////////////////////////////////////////////

static const auto HiveTracingService = TString("HiveManager");
static const auto ClientHostAnnotation = TString("client_host");

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
        Y_ASSERT(!*HiveMutation);
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
        const TCellId& selfCellId,
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
        , Config_(config)
        , CellDirectory_(cellDirectory)
        , HydraManager_(hydraManager)
    {
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(Ping));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(SyncCells));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(PostMessages));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(SendMessages));

        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraAcknowledgeMessages, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraPostMessages, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraSendMessages, Unretained(this)));
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

        OrchidService_ = CreateOrchidService(automatonInvoker);
    }

    IServicePtr GetRpcService()
    {
        return this;
    }

    const TCellId& GetSelfCellId() const
    {
        return SelfCellId_;
    }

    TMailbox* CreateMailbox(const TCellId& cellId)
    {
        auto mailboxHolder = std::make_unique<TMailbox>(cellId);
        auto* mailbox = MailboxMap_.Insert(cellId, std::move(mailboxHolder));

        if (!IsRecovery()) {
            SendPeriodicPing(mailbox);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Mailbox created (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            mailbox->GetCellId());
        return mailbox;
    }

    TMailbox* GetOrCreateMailbox(const TCellId& cellId)
    {
        auto* mailbox = MailboxMap_.Find(cellId);
        if (!mailbox) {
            mailbox = CreateMailbox(cellId);
        }
        return mailbox;
    }

    TMailbox* GetMailboxOrThrow(const TCellId& cellId)
    {
        auto* mailbox = FindMailbox(cellId);
        if (!mailbox) {
            THROW_ERROR_EXCEPTION("No such mailbox %v",
                cellId);
        }
        return mailbox;
    }

    void RemoveMailbox(TMailbox* mailbox)
    {
        auto cellId = mailbox->GetCellId();
        MailboxMap_.Remove(cellId);
        LOG_INFO_UNLESS(IsRecovery(), "Mailbox removed (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            cellId);
    }

    void PostMessage(TMailbox* mailbox, TRefCountedEncapsulatedMessagePtr message, bool reliable)
    {
        PostMessage(TMailboxList{mailbox}, std::move(message), reliable);
    }

    void PostMessage(const TMailboxList& mailboxes, TRefCountedEncapsulatedMessagePtr message, bool reliable)
    {
        if (reliable) {
            ReliablePostMessage(mailboxes, std::move(message));
        } else {
            UnreliablePostMessage(mailboxes, std::move(message));
        }
    }

    void PostMessage(TMailbox* mailbox, const ::google::protobuf::MessageLite& message, bool reliable)
    {
        auto encapsulatedMessage = SerializeMessage(message);
        PostMessage(mailbox, std::move(encapsulatedMessage), reliable);
    }

    void PostMessage(const TMailboxList& mailboxes, const ::google::protobuf::MessageLite& message, bool reliable)
    {
        auto encapsulatedMessage = SerializeMessage(message);
        PostMessage(mailboxes, std::move(encapsulatedMessage), reliable);
    }


    TFuture<void> SyncWith(const TCellId& cellId)
    {
        YCHECK(EpochAutomatonInvoker_);

        auto proxy = FindHiveProxy(cellId);
        if (!proxy) {
            return MakeFuture(TError(
                NRpc::EErrorCode::Unavailable,
                "Cannot synchronize with cell %v since it is not yet connected",
                cellId));
        }

        LOG_DEBUG("Synchronizing with another instance (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            cellId);

        auto req = proxy->Ping();
        req->SetTimeout(Config_->PingRpcTimeout);
        ToProto(req->mutable_src_cell_id(), SelfCellId_);

        return req->Invoke().Apply(
            BIND(&TImpl::OnSyncPingResponse, MakeStrong(this), cellId)
                .AsyncVia(EpochAutomatonInvoker_));
    }

    IYPathServicePtr GetOrchidService()
    {
        return OrchidService_;
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Mailbox, TMailbox);

private:
    const TCellId SelfCellId_;
    const THiveManagerConfigPtr Config_;
    const TCellDirectoryPtr CellDirectory_;
    const IHydraManagerPtr HydraManager_;

    IYPathServicePtr OrchidService_;

    TEntityMap<TMailbox> MailboxMap_;
    THashMap<TCellId, TMessageId> CellIdToNextTransientIncomingMessageId_;


    // RPC handlers.

    DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto, Ping)
    {
        auto srcCellId = FromProto<TCellId>(request->src_cell_id());

        context->SetRequestInfo("SrcCellId: %v, DstCellId: %v",
            srcCellId,
            SelfCellId_);

        ValidatePeer(EPeerKind::Leader);

        auto* mailbox = FindMailbox(srcCellId);
        auto lastOutcomingMessageId = mailbox
            ? MakeNullable(mailbox->GetFirstOutcomingMessageId() + static_cast<int>(mailbox->OutcomingMessages().size()) - 1)
            : Null;

        if (lastOutcomingMessageId) {
            response->set_last_outcoming_message_id(*lastOutcomingMessageId);
        }

        context->SetResponseInfo("NextTransientIncomingMessageId: %v",
            lastOutcomingMessageId);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto, SyncCells)
    {
        context->SetRequestInfo();

        ValidatePeer(EPeerKind::LeaderOrFollower);

        auto registeredCellList = CellDirectory_->GetRegisteredCells();
        THashMap<TCellId, TCellInfo> registeredCellMap;
        for (const auto& cellInfo : registeredCellList) {
            YCHECK(registeredCellMap.insert(std::make_pair(cellInfo.CellId, cellInfo)).second);
        }

        THashSet<TCellId> missingCellIds;
        for (const auto& cellInfo : registeredCellList) {
            YCHECK(missingCellIds.insert(cellInfo.CellId).second);
        }

        auto requestReconfigure = [&] (const TCellDescriptor& cellDescriptor, int oldVersion) {
            LOG_DEBUG("Requesting cell reconfiguration (CellId: %v, ConfigVersion: %v -> %v)",
                cellDescriptor.CellId,
                oldVersion,
                cellDescriptor.ConfigVersion);
            auto* protoInfo = response->add_cells_to_reconfigure();
            ToProto(protoInfo->mutable_cell_descriptor(), cellDescriptor);
        };

        auto requestUnregister = [&] (const TCellId& cellId) {
            LOG_DEBUG("Requesting cell unregistration (CellId: %v)",
                cellId);
            auto* unregisterInfo = response->add_cells_to_unregister();
            ToProto(unregisterInfo->mutable_cell_id(), cellId);
        };

        for (const auto& protoCellInfo : request->known_cells()) {
            auto cellId = FromProto<TCellId>(protoCellInfo.cell_id());
            auto it = registeredCellMap.find(cellId);
            if (it == registeredCellMap.end()) {
                requestUnregister(cellId);
            } else {
                YCHECK(missingCellIds.erase(cellId) == 1);
                const auto& cellInfo = it->second;
                if (protoCellInfo.config_version() < cellInfo.ConfigVersion) {
                    auto cellDescriptor = CellDirectory_->FindDescriptor(cellId);
                    // If cell descriptor is already missing then just skip this cell and
                    // postpone it for another heartbeat.
                    if (cellDescriptor) {
                        requestReconfigure(*cellDescriptor, protoCellInfo.config_version());
                    }
                }
            }
        }

        for (const auto& cellId : missingCellIds) {
            auto cellDescriptor = CellDirectory_->FindDescriptor(cellId);
            // See above.
            if (cellDescriptor) {
                requestReconfigure(*cellDescriptor, -1);
            }
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto, PostMessages)
    {
        auto srcCellId = FromProto<TCellId>(request->src_cell_id());
        auto firstMessageId = request->first_message_id();
        int messageCount = request->messages_size();

        context->SetRequestInfo("SrcCellId: %v, DstCellId: %v, MessageIds: %v-%v",
            srcCellId,
            SelfCellId_,
            firstMessageId,
            firstMessageId + messageCount - 1);

        ValidatePeer(EPeerKind::Leader);

        auto* nextTransientIncomingMessageId = GetNextTransientIncomingMessageIdPtr(srcCellId);
        if (*nextTransientIncomingMessageId == firstMessageId && messageCount > 0) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Committing reliable incoming messages (SrcCellId: %v, DstCellId: %v, "
                "MessageIds: %v-%v)",
                srcCellId,
                SelfCellId_,
                firstMessageId,
                firstMessageId + messageCount - 1);

            *nextTransientIncomingMessageId += messageCount;
            CreatePostMessagesMutation(*request)
                ->CommitAndLog(Logger);
        }
        response->set_next_transient_incoming_message_id(*nextTransientIncomingMessageId);

        auto nextPersistentIncomingMessageId = GetNextPersistentIncomingMessageId(srcCellId);
        if (nextPersistentIncomingMessageId) {
            response->set_next_persistent_incoming_message_id(*nextPersistentIncomingMessageId);
        }

        context->SetResponseInfo("NextPersistentIncomingMessageId: %v, NextTransientIncomingMessageId: %v",
            nextPersistentIncomingMessageId,
            *nextTransientIncomingMessageId);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NHiveClient::NProto, SendMessages)
    {
        auto srcCellId = FromProto<TCellId>(request->src_cell_id());
        int messageCount = request->messages_size();

        context->SetRequestInfo("SrcCellId: %v, DstCellId: %v, MessageCount: %v",
            srcCellId,
            SelfCellId_,
            messageCount);

        ValidatePeer(EPeerKind::Leader);

        LOG_DEBUG_UNLESS(IsRecovery(), "Committing unreliable incoming messages (SrcCellId: %v, DstCellId: %v, "
            "MessageCount: %v)",
            srcCellId,
            SelfCellId_,
            messageCount);

        CreateSendMessagesMutation(context)
            ->CommitAndReply(context);
    }


    // Hydra handlers.

    void HydraAcknowledgeMessages(NHiveServer::NProto::TReqAcknowledgeMessages* request)
    {
        auto cellId = FromProto<TCellId>(request->cell_id());
        auto* mailbox = FindMailbox(cellId);
        if (!mailbox) {
            return;
        }

        mailbox->SetAcknowledgeInProgress(false);

        auto nextPersistentIncomingMessageId = request->next_persistent_incoming_message_id();
        auto acknowledgeCount = nextPersistentIncomingMessageId - mailbox->GetFirstOutcomingMessageId();
        if (acknowledgeCount <= 0) {
            LOG_DEBUG_UNLESS(IsRecovery(), "No messages acknowledged (SrcCellId: %v, DstCellId: %v, "
                "NextPersistentIncomingMessageId: %v, FirstOutcomingMessageId: %v)",
                SelfCellId_,
                mailbox->GetCellId(),
                nextPersistentIncomingMessageId,
                mailbox->GetFirstOutcomingMessageId());
            return;
        }

        auto& outcomingMessages = mailbox->OutcomingMessages();
        if (acknowledgeCount > outcomingMessages.size()) {
            LOG_ERROR_UNLESS(IsRecovery(), "Requested to acknowledge too many messages (SrcCellId: %v, DstCellId: %v, "
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

        LOG_DEBUG_UNLESS(IsRecovery(), "Messages acknowledged (SrcCellId: %v, DstCellId: %v, "
            "FirstOutcomingMessageId: %v)",
            SelfCellId_,
            mailbox->GetCellId(),
            mailbox->GetFirstOutcomingMessageId());
    }

    void HydraPostMessages(NHiveClient::NProto::TReqPostMessages* request)
    {
        auto srcCellId = FromProto<TCellId>(request->src_cell_id());
        auto firstMessageId = request->first_message_id();
        auto* mailbox = FindMailbox(srcCellId);
        if (!mailbox) {
            if (firstMessageId != 0) {
                LOG_ERROR_UNLESS(IsRecovery(), "Mailbox %v does not exist; expecting message 0 but got %v",
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
        auto srcCellId = FromProto<TCellId>(request->src_cell_id());
        auto* mailbox = GetMailboxOrThrow(srcCellId);
        ApplyUnreliableIncomingMessages(mailbox, request);
    }

    void HydraUnregisterMailbox(NHiveServer::NProto::TReqUnregisterMailbox* request)
    {
        auto cellId = FromProto<TCellId>(request->cell_id());
        auto* mailbox = FindMailbox(cellId);
        if (mailbox) {
            RemoveMailbox(mailbox);
        }
    }


    std::unique_ptr<THiveServiceProxy> FindHiveProxy(TMailbox* mailbox)
    {
        return FindHiveProxy(mailbox->GetCellId());
    }

    std::unique_ptr<THiveServiceProxy> FindHiveProxy(const TCellId& cellId)
    {
        auto channel = CellDirectory_->FindChannel(cellId);
        if (!channel) {
            return nullptr;
        }

        return std::make_unique<THiveServiceProxy>(channel);
    }


    void ReliablePostMessage(const TMailboxList& mailboxes, const TRefCountedEncapsulatedMessagePtr& message)
    {
        // A typical mistake is to try sending a Hive message outside of a mutation.
        YCHECK(HasMutationContext());

        AnnotateWithTraceContext(message.Get());

        TStringBuilder logMessageBuilder;
        logMessageBuilder.AppendFormat("Reliable outcoming message added (MutationType: %v, SrcCellId: %v, DstCellIds: {",
            message->type(),
            SelfCellId_);

        for (auto* mailbox : mailboxes) {
            auto messageId =
                mailbox->GetFirstOutcomingMessageId() +
                mailbox->OutcomingMessages().size();

            mailbox->OutcomingMessages().push_back(message);

            if (mailbox != mailboxes.front()) {
                logMessageBuilder.AppendString(STRINGBUF(", "));
            }
            logMessageBuilder.AppendFormat("%v=>%v",
                mailbox->GetCellId(),
                messageId);

            MaybePostOutcomingMessages(mailbox, false);
        }

        logMessageBuilder.AppendString(STRINGBUF("})"));
        LOG_DEBUG_UNLESS(IsRecovery(), logMessageBuilder.Flush());

        for (auto* mailbox : mailboxes) {
            MaybePostOutcomingMessages(mailbox, false);
        }
    }

    void UnreliablePostMessage(const TMailboxList& mailboxes, const TRefCountedEncapsulatedMessagePtr& message)
    {
        TStringBuilder logMessageBuilder;
        logMessageBuilder.AppendFormat("Sending unreliable outcoming message (MutationType: %v, SrcCellId: %v, DstCellIds: [",
            message->type(),
            SelfCellId_);

        for (auto* mailbox : mailboxes) {
            if (!mailbox->GetConnected()) {
                continue;
            }

            auto proxy = FindHiveProxy(mailbox);
            if (!proxy) {
                continue;
            }

            if (mailbox != mailboxes.front()) {
                logMessageBuilder.AppendString(STRINGBUF(", "));
            }
            logMessageBuilder.AppendFormat("%v", mailbox->GetCellId());

            auto req = proxy->SendMessages();
            req->SetTimeout(Config_->SendRpcTimeout);
            ToProto(req->mutable_src_cell_id(), SelfCellId_);
            *req->add_messages() = *message;
            AnnotateWithTraceContext(req->mutable_messages(0));

            req->Invoke().Subscribe(
                BIND(&TImpl::OnSendMessagesResponse, MakeStrong(this), mailbox->GetCellId())
                    .Via(EpochAutomatonInvoker_));
        }

        logMessageBuilder.AppendString(STRINGBUF("])"));
        LOG_DEBUG_UNLESS(IsRecovery(), logMessageBuilder.Flush());
    }


    void SetMailboxConnected(TMailbox* mailbox)
    {
        if (mailbox->GetConnected()) {
            return;
        }

        mailbox->SetConnected(true);
        YCHECK(mailbox->SyncRequests().empty());
        mailbox->SetFirstInFlightOutcomingMessageId(mailbox->GetFirstOutcomingMessageId());
        YCHECK(mailbox->GetInFlightOutcomingMessageCount() == 0);

        LOG_INFO("Mailbox connected (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            mailbox->GetCellId());

        MaybePostOutcomingMessages(mailbox, true);
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

        LOG_INFO("Mailbox disconnected (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            mailbox->GetCellId());
    }

    void ResetMailboxes()
    {
        for (const auto& pair : MailboxMap_) {
            auto* mailbox = pair.second;
            SetMailboxDisconnected(mailbox);
            mailbox->SetAcknowledgeInProgress(false);
        }
        CellIdToNextTransientIncomingMessageId_.clear();
    }


    TMessageId* GetNextTransientIncomingMessageIdPtr(const TCellId& cellId)
    {
        auto it = CellIdToNextTransientIncomingMessageId_.find(cellId);
        if (it != CellIdToNextTransientIncomingMessageId_.end()) {
            return &it->second;
        }

        return &CellIdToNextTransientIncomingMessageId_.emplace(
            cellId,
            GetNextPersistentIncomingMessageId(cellId).Get(0)).first->second;
    }

    TMessageId GetNextTransientIncomingMessageId(TMailbox* mailbox)
    {
        auto it = CellIdToNextTransientIncomingMessageId_.find(mailbox->GetCellId());
        return it == CellIdToNextTransientIncomingMessageId_.end()
            ? mailbox->GetNextIncomingMessageId()
            : it->second;
    }

    TNullable<TMessageId> GetNextPersistentIncomingMessageId(const TCellId& cellId)
    {
        auto* mailbox = FindMailbox(cellId);
        return mailbox ? MakeNullable(mailbox->GetNextIncomingMessageId()) : Null;
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
            YCHECK(!mailbox->GetConnected());
            SendPeriodicPing(mailbox);
        }
    }

    void OnPeriodicPingTick(const TCellId& cellId)
    {
        auto* mailbox = FindMailbox(cellId);
        if (!mailbox) {
            return;
        }

        SendPeriodicPing(mailbox);
    }

    void SendPeriodicPing(TMailbox* mailbox)
    {
        const auto& cellId = mailbox->GetCellId();

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

        auto proxy = FindHiveProxy(mailbox);
        if (!proxy) {
            // Let's register a dummy descriptor so as to ask about it during the next sync.
            CellDirectory_->RegisterCell(cellId);
            SchedulePeriodicPing(mailbox);
            return;
        }

        LOG_DEBUG("Sending periodic ping (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            mailbox->GetCellId());

        auto req = proxy->Ping();
        req->SetTimeout(Config_->PingRpcTimeout);
        ToProto(req->mutable_src_cell_id(), SelfCellId_);

        req->Invoke().Subscribe(
            BIND(&TImpl::OnPeriodicPingResponse, MakeStrong(this), mailbox->GetCellId())
                .Via(EpochAutomatonInvoker_));
    }

    void OnPeriodicPingResponse(const TCellId& cellId, const THiveServiceProxy::TErrorOrRspPingPtr& rspOrError)
    {
        auto* mailbox = FindMailbox(cellId);
        if (!mailbox) {
            return;
        }

        SchedulePeriodicPing(mailbox);

        if (!rspOrError.IsOK()) {
            LOG_DEBUG(rspOrError, "Periodic ping failed (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                mailbox->GetCellId());
            return;
        }

        const auto& rsp = rspOrError.Value();
        auto lastOutcomingMessageId = rsp->has_last_outcoming_message_id()
            ? MakeNullable(rsp->last_outcoming_message_id())
            : Null;

        LOG_DEBUG("Periodic ping succeeded (SrcCellId: %v, DstCellId: %v, LastOutcomingMessageId: %v)",
            SelfCellId_,
            mailbox->GetCellId(),
            lastOutcomingMessageId);

        SetMailboxConnected(mailbox);
    }


    TFuture<void> OnSyncPingResponse(const TCellId& cellId, const THiveServiceProxy::TErrorOrRspPingPtr& rspOrError)
    {
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
            LOG_DEBUG("Remote instance has no mailbox; no synchronization needed (SrcCellId: %v, DstCellId: %v)",
                cellId,
                SelfCellId_);
            return VoidFuture;
        }

        auto messageId = rsp->last_outcoming_message_id();
        if (messageId < mailbox->GetNextIncomingMessageId()) {
            LOG_DEBUG("Already synchronized with remote instance (SrcCellId: %v, DstCellId: %v, "
                "SyncMessageId: %v, NextPersistentIncomingMessageId: %v)",
                cellId,
                SelfCellId_,
                messageId,
                mailbox->GetNextIncomingMessageId());
            return VoidFuture;
        }

        LOG_DEBUG("Waiting for synchronization with remote instance (SrcCellId: %v, DstCellId: %v, "
            "SyncMessageId: %v, NextPersistentIncomingMessageId: %v)",
            cellId,
            SelfCellId_,
            messageId,
            mailbox->GetNextIncomingMessageId());

        return RegisterSyncRequest(mailbox, messageId);
    }

    TFuture<void> RegisterSyncRequest(TMailbox* mailbox, TMessageId messageId)
    {
        auto& syncRequests = mailbox->SyncRequests();

        auto it = syncRequests.find(messageId);
        if (it != syncRequests.end()) {
            const auto& entry = it->second;
            return entry.Promise.ToFuture();
        }

        TMailbox::TSyncRequest request;
        request.MessageId = messageId;
        request.Promise = NewPromise<void>();

        YCHECK(syncRequests.insert(std::make_pair(messageId, request)).second);
        return request.Promise.ToFuture();
    }

    void FlushSyncRequests(TMailbox* mailbox)
    {
        auto& syncRequests = mailbox->SyncRequests();
        while (!syncRequests.empty()) {
            auto it = syncRequests.begin();
            auto messageId = it->first;
            if (messageId >= mailbox->GetNextIncomingMessageId()) {
                break;
            }

            auto& request = it->second;

            LOG_DEBUG("Synchronization complete (SrcCellId: %v, DstCellId: %v, MessageId: %v)",
                SelfCellId_,
                mailbox->GetCellId(),
                messageId);

            request.Promise.Set();
            syncRequests.erase(it);
        }
    }

    void OnIdlePostOutcomingMessages(const TCellId& cellId)
    {
        auto* mailbox = FindMailbox(cellId);
        if (!mailbox) {
            return;
        }
        MaybePostOutcomingMessages(mailbox, true);
    }

    void MaybePostOutcomingMessages(TMailbox* mailbox, bool allowIdle)
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
        YCHECK(firstMessageId >= mailbox->GetFirstOutcomingMessageId());
        YCHECK(firstMessageId <= mailbox->GetFirstOutcomingMessageId() + outcomingMessages.size());

        TDelayedExecutor::CancelAndClear(mailbox->IdlePostCookie());
        if (!allowIdle && firstMessageId == mailbox->GetFirstOutcomingMessageId() + outcomingMessages.size()) {
            mailbox->IdlePostCookie() = TDelayedExecutor::Submit(
                BIND(&TImpl::OnIdlePostOutcomingMessages, MakeWeak(this), mailbox->GetCellId())
                    .Via(EpochAutomatonInvoker_),
                Config_->IdlePostPeriod);
            return;
        }

        auto proxy = FindHiveProxy(mailbox);
        if (!proxy) {
            return;
        }

        auto req = proxy->PostMessages();
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
            *req->add_messages() = *message;
            messagesToPost += 1;
            bytesToPost += message->ByteSize();
        }

        mailbox->SetInFlightOutcomingMessageCount(messagesToPost);
        mailbox->SetPostInProgress(true);

        if (messagesToPost == 0) {
            LOG_DEBUG("Checking mailbox synchronization (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                mailbox->GetCellId());
        } else {
            LOG_DEBUG("Posting reliable outcoming messages (SrcCellId: %v, DstCellId: %v, MessageIds: %v-%v)",
                SelfCellId_,
                mailbox->GetCellId(),
                firstMessageId,
                firstMessageId + messagesToPost - 1);
        }

        req->Invoke().Subscribe(
            BIND(&TImpl::OnPostMessagesResponse, MakeStrong(this), mailbox->GetCellId())
                .Via(EpochAutomatonInvoker_));
    }

    void OnPostMessagesResponse(const TCellId& cellId, const THiveServiceProxy::TErrorOrRspPostMessagesPtr& rspOrError)
    {
        auto* mailbox = FindMailbox(cellId);
        if (!mailbox) {
            return;
        }

        if (!mailbox->GetPostInProgress()) {
            return;
        }

        mailbox->SetInFlightOutcomingMessageCount(0);
        mailbox->SetPostInProgress(false);

        if (!rspOrError.IsOK()) {
            LOG_DEBUG(rspOrError, "Failed to post reliable outcoming messages (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                mailbox->GetCellId());
            SetMailboxDisconnected(mailbox);
            return;
        }

        const auto& rsp = rspOrError.Value();
        auto nextPersistentIncomingMessageId = rsp->has_next_persistent_incoming_message_id()
            ? MakeNullable(rsp->next_persistent_incoming_message_id())
            : Null;
        auto nextTransientIncomingMessageId = rsp->next_transient_incoming_message_id();
        LOG_DEBUG("Outcoming reliable messages posted (SrcCellId: %v, DstCellId: %v, "
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

        MaybePostOutcomingMessages(mailbox, false);
    }

    void OnSendMessagesResponse(const TCellId& cellId, const THiveServiceProxy::TErrorOrRspSendMessagesPtr& rspOrError)
    {
        auto* mailbox = FindMailbox(cellId);
        if (!mailbox) {
            return;
        }

        if (!rspOrError.IsOK()) {
            LOG_DEBUG(rspOrError, "Failed to send unreliable outcoming messages (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                mailbox->GetCellId());
            SetMailboxDisconnected(mailbox);
            return;
        }

        LOG_DEBUG("Outcoming unreliable messages sent successfully (SrcCellId: %v, DstCellId: %v)",
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

    std::unique_ptr<TMutation> CreateSendMessagesMutation(TCtxSendMessagesPtr context)
    {
        return CreateMutation(
            HydraManager_,
            std::move(context),
            &TImpl::HydraSendMessages,
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
            LOG_ERROR_UNLESS(IsRecovery(), "Destination is out of sync: requested to receive already truncated messages (SrcCellId: %v, DstCellId: %v, "
                "RequestedMessageId: %v, FirstOutcomingMessageId: %v)",
                SelfCellId_,
                mailbox->GetCellId(),
                requestedMessageId,
                mailbox->GetFirstOutcomingMessageId());
            SetMailboxDisconnected(mailbox);
            return false;
        }

        if (requestedMessageId > mailbox->GetFirstOutcomingMessageId() + mailbox->OutcomingMessages().size()) {
            LOG_ERROR_UNLESS(IsRecovery(), "Destination is out of sync: requested to receive nonexisting messages (SrcCellId: %v, DstCellId: %v, "
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

        LOG_DEBUG_UNLESS(IsRecovery(), "Committing reliable messages acknowledgement (SrcCellId: %v, DstCellId: %v, "
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
        if (messageId != mailbox->GetNextIncomingMessageId()) {
            LOG_ERROR_UNLESS(IsRecovery(), "Unexpected error: attempt to apply an out-of-order message (SrcCellId: %v, DstCellId: %v, "
                "ExpectedMessageId: %v, ActualMessageId: %v, MutationType: %v)",
                mailbox->GetCellId(),
                SelfCellId_,
                mailbox->GetNextIncomingMessageId(),
                messageId,
                message.type());
            return;
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Applying reliable incoming message (SrcCellId: %v, DstCellId: %v, MessageId: %v, MutationType: %v)",
            mailbox->GetCellId(),
            SelfCellId_,
            messageId,
            message.type());

        ApplyMessage(message);

        mailbox->SetNextIncomingMessageId(messageId + 1);

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
        LOG_DEBUG_UNLESS(IsRecovery(), "Applying unreliable incoming message (SrcCellId: %v, DstCellId: %v, MutationType: %v)",
            mailbox->GetCellId(),
            SelfCellId_,
            message.type());
        ApplyMessage(message);
    }

    void ApplyMessage(const TEncapsulatedMessage& message)
    {
        TMutationRequest request;
        request.Type = message.type();
        request.Data = TSharedRef::FromString(message.data());

        auto traceContext = GetTraceContext(message);
        TTraceContextGuard traceContextGuard(traceContext);

        TRACE_ANNOTATION(
            traceContext,
            HiveTracingService,
            request.Type,
            ServerReceiveAnnotation);

        {
            TMutationContext mutationContext(GetCurrentMutationContext(), request);
            TMutationContextGuard mutationContextGuard(&mutationContext);

            THiveMutationGuard hiveMutationGuard;

            static_cast<IAutomaton*>(Automaton_)->ApplyMutation(&mutationContext);
        }

        TRACE_ANNOTATION(
            traceContext,
            HiveTracingService,
            request.Type,
            ServerSendAnnotation);
    }


    static TTraceContext GetTraceContext(const TEncapsulatedMessage& message)
    {
        if (!message.has_trace_id()) {
            return TTraceContext();
        }

        return TTraceContext(
            message.trace_id(),
            message.span_id(),
            message.parent_span_id());
    }

    static void AnnotateWithTraceContext(TEncapsulatedMessage* message)
    {
        auto traceContext = CreateChildTraceContext();
        if (!traceContext.IsEnabled())
            return;

        TRACE_ANNOTATION(
            traceContext,
            HiveTracingService,
            message->type(),
            ClientSendAnnotation);

        TRACE_ANNOTATION(
            traceContext,
            ClientHostAnnotation,
            GetLocalHostName());

        message->set_trace_id(traceContext.GetTraceId());
        message->set_span_id(traceContext.GetSpanId());
        message->set_parent_span_id(traceContext.GetParentSpanId());
    }


    // NB: Leader must wait until it is active before reconnecting mailboxes
    // since no commits are possible before this point.
    virtual void OnLeaderActive() override
    {
        TCompositeAutomatonPart::OnLeaderRecoveryComplete();
        ReconnectMailboxes();
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
            version == 2 ||
            version == 3;
    }

    virtual int GetCurrentSnapshotVersion() override
    {
        return 3;
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
    }

    void LoadKeys(TLoadContext& context)
    {
        MailboxMap_.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        MailboxMap_.LoadValues(context);
    }


    // THydraServiceBase overrides.
    virtual IHydraManagerPtr GetHydraManager() override
    {
        return HydraManager_;
    }


    IYPathServicePtr CreateOrchidService(IInvokerPtr automatonInvoker)
    {
        auto producer = BIND(&TImpl::BuildOrchidYson, MakeWeak(this));
        return IYPathService::FromProducer(producer)
            ->Via(automatonInvoker)
            ->Cached(TDuration::Seconds(1));
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
                            .Item("next_persistent_incoming_message_id").Value(mailbox->GetNextIncomingMessageId())
                            .Item("next_transient_incoming_message_id").Value(GetNextTransientIncomingMessageId(mailbox))
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
    const TCellId& selfCellId,
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

THiveManager::~THiveManager() = default;

IServicePtr THiveManager::GetRpcService()
{
    return Impl_->GetRpcService();
}

const TCellId& THiveManager::GetSelfCellId() const
{
    return Impl_->GetSelfCellId();
}

TMailbox* THiveManager::CreateMailbox(const TCellId& cellId)
{
    return Impl_->CreateMailbox(cellId);
}

TMailbox* THiveManager::GetOrCreateMailbox(const TCellId& cellId)
{
    return Impl_->GetOrCreateMailbox(cellId);
}

TMailbox* THiveManager::GetMailboxOrThrow(const TCellId& cellId)
{
    return Impl_->GetMailboxOrThrow(cellId);
}

void THiveManager::RemoveMailbox(TMailbox* mailbox)
{
    Impl_->RemoveMailbox(mailbox);
}

void THiveManager::PostMessage(TMailbox* mailbox, TRefCountedEncapsulatedMessagePtr message, bool reliable)
{
    Impl_->PostMessage(mailbox, std::move(message), reliable);
}

void THiveManager::PostMessage(const TMailboxList& mailboxes, TRefCountedEncapsulatedMessagePtr message, bool reliable)
{
    Impl_->PostMessage(mailboxes, std::move(message), reliable);
}

void THiveManager::PostMessage(TMailbox* mailbox, const ::google::protobuf::MessageLite& message, bool reliable)
{
    Impl_->PostMessage(mailbox, message, reliable);
}

void THiveManager::PostMessage(const TMailboxList& mailboxes, const ::google::protobuf::MessageLite& message, bool reliable)
{
    Impl_->PostMessage(mailboxes, message, reliable);
}

TFuture<void> THiveManager::SyncWith(const TCellId& cellId)
{
    return Impl_->SyncWith(cellId);
}

IYPathServicePtr THiveManager::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

DELEGATE_ENTITY_MAP_ACCESSORS(THiveManager, Mailbox, TMailbox, *Impl_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
