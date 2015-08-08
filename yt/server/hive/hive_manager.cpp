#include "stdafx.h"
#include "hive_manager.h"
#include "config.h"
#include "mailbox.h"
#include "helpers.h"

#include <core/misc/address.h>

#include <core/rpc/service_detail.h>
#include <core/rpc/server.h>

#include <core/concurrency/delayed_executor.h>

#include <core/ytree/fluent.h>

#include <core/tracing/trace_context.h>

#include <core/rpc/rpc.pb.h>

#include <ytlib/hydra/peer_channel.h>
#include <ytlib/hydra/config.h>

#include <ytlib/hive/private.h>
#include <ytlib/hive/cell_directory.h>
#include <ytlib/hive/hive_service_proxy.h>

#include <server/election/election_manager.h>

#include <server/hydra/composite_automaton.h>
#include <server/hydra/hydra_manager.h>
#include <server/hydra/mutation_context.h>
#include <server/hydra/mutation.h>
#include <server/hydra/hydra_service.h>
#include <server/hydra/rpc_helpers.h>

namespace NYT {
namespace NHive {

using namespace NRpc;
using namespace NRpc::NProto;
using namespace NHydra;
using namespace NHydra::NProto;
using namespace NConcurrency;
using namespace NHive::NProto;
using namespace NYson;
using namespace NYTree;
using namespace NTracing;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto HiveTracingService = Stroka("HiveManager");
static const auto ClientHostAnnotation = Stroka("client_host");

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
            hydraManager,
            automatonInvoker,
            TServiceId(THiveServiceProxy::GetServiceName(), selfCellId),
            HiveLogger,
            THiveServiceProxy::GetProtocolVersion())
        , TCompositeAutomatonPart(
            hydraManager,
            automaton,
            automatonInvoker)
        , SelfCellId_(selfCellId)
        , Config_(config)
        , CellDirectory_(cellDirectory)
    {
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(Ping));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(SyncCells));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(PostMessages));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(SendMessages));

        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraAcknowledgeMessages, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraPostMessages, Unretained(this), nullptr));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraSendMessages, Unretained(this), nullptr));
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
        
        if (IsLeader()) {
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

    void RemoveMailbox(const TCellId& cellId)
    {
        MailboxMap_.Remove(cellId);
        LOG_INFO_UNLESS(IsRecovery(), "Mailbox removed (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            cellId);
    }

    void PostMessage(TMailbox* mailbox, const TEncapsulatedMessage& message, bool reliable)
    {
        if (reliable) {
            ReliablePostMessage(mailbox, message);
        } else {
            UnreliablePostMessage(mailbox, message);
        }
    }

    void PostMessage(TMailbox* mailbox, const ::google::protobuf::MessageLite& message, bool reliable)
    {
        PostMessage(mailbox, SerializeMessage(message), reliable);
    }


    TFuture<void> SyncWith(const TCellId& cellId)
    {
        YCHECK(EpochAutomatonInvoker_);
        
        auto proxy = FindHiveProxy(cellId);
        if (!proxy) {
            return MakeFuture(TError("Cannot connect to cell %v",
                cellId));
        }

        LOG_DEBUG("Synchronizing with another instance (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            cellId);

        auto req = proxy->Ping();
        ToProto(req->mutable_src_cell_id(), SelfCellId_);

        return req->Invoke().Apply(
            BIND(&TImpl::OnSyncPingResponse, MakeStrong(this), cellId)
                .AsyncVia(EpochAutomatonInvoker_));
    }


    void BuildOrchidYson(IYsonConsumer* consumer)
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("mailboxes").DoMapFor(MailboxMap_, [&] (TFluentMap fluent, const std::pair<TCellId, TMailbox*>& pair) {
                    auto* mailbox = pair.second;
                    fluent
                        .Item(ToString(mailbox->GetCellId())).BeginMap()
                            .Item("outcoming_message_count").Value(mailbox->OutcomingMessages().size())
                            .Item("first_outcoming_message_id").Value(mailbox->GetFirstOutcomingMessageId())
                            .Item("last_incoming_message_id").Value(mailbox->GetLastIncomingMessageId())
                            .Item("incoming_message_ids").BeginList()
                                .DoFor(mailbox->IncomingMessages(), [&] (TFluentList fluent, const TMailbox::TIncomingMessageMap::value_type& pair) {
                                    fluent
                                        .Item().Value(pair.first);
                                })
                            .EndList()
                            .Item("post_messages_in_flight").Value(mailbox->GetPostMessagesInFlight())
                        .EndMap();
                })
            .EndMap();
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Mailbox, TMailbox, TCellId);

private:
    const TCellId SelfCellId_;
    const THiveManagerConfigPtr Config_;
    const TCellDirectoryPtr CellDirectory_;

    TEntityMap<TCellId, TMailbox> MailboxMap_;
    

    // RPC handlers.

    DECLARE_RPC_SERVICE_METHOD(NProto, Ping)
    {
        auto srcCellId = FromProto<TCellId>(request->src_cell_id());

        context->SetRequestInfo("SrcCellId: %v, DstCellId: %v",
            srcCellId,
            SelfCellId_);

        auto* mailbox = FindMailbox(srcCellId);
        int lastIncomingMessageId = mailbox
            ? mailbox->GetLastIncomingMessageId()
            : -1;
        int lastOutcomingMessageId = mailbox
            ? mailbox->GetFirstOutcomingMessageId() + mailbox->OutcomingMessages().size() - 1
            : -1;

        response->set_last_incoming_message_id(lastIncomingMessageId);
        response->set_last_outcoming_message_id(lastOutcomingMessageId);

        context->SetResponseInfo("LastIncomingMessageId: %v, LastOutcomingMessageId: %v",
            lastIncomingMessageId,
            lastOutcomingMessageId);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, SyncCells)
    {
        context->SetRequestInfo();

        auto registeredCellList = CellDirectory_->GetRegisteredCells();
        yhash_map<TCellId, NHive::TCellInfo> registeredCellMap;
        for (const auto& cellInfo : registeredCellList) {
            YCHECK(registeredCellMap.insert(std::make_pair(cellInfo.CellId, cellInfo)).second);
        }

        yhash_set<TCellId> missingCellIds;
        for (const auto& cellInfo : registeredCellList) {
            YCHECK(missingCellIds.insert(cellInfo.CellId).second);
        }

        auto requestReconfigure = [&] (const NHive::TCellDescriptor& cellDescriptor, int oldVersion) {
            LOG_DEBUG("Requesting cell reconfiguration (CellId: %v, ConfigVersion: %v->%v)",
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

    DECLARE_RPC_SERVICE_METHOD(NProto, PostMessages)
    {
        auto srcCellId = FromProto<TCellId>(request->src_cell_id());
        int firstMessageId = request->first_message_id();

        context->SetRequestInfo("SrcCellId: %v, DstCellId: %v, MessageIds: %v-%v",
            srcCellId,
            SelfCellId_,
            firstMessageId,
            firstMessageId + request->messages_size() - 1);
        
        CreatePostMessagesMutation(context)
            ->Commit()
             .Subscribe(CreateRpcResponseHandler(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, SendMessages)
    {
        auto srcCellId = FromProto<TCellId>(request->src_cell_id());

        context->SetRequestInfo("SrcCellId: %v, DstCellId: %v, MessageCount: %v",
            srcCellId,
            SelfCellId_,
            request->messages_size());

        CreateSendMessagesMutation(context)
            ->Commit()
             .Subscribe(CreateRpcResponseHandler(context));
    }


    // Hydra handlers.

    void HydraAcknowledgeMessages(const TReqAcknowledgeMessages& request)
    {
        auto cellId = FromProto<TCellId>(request.cell_id());
        auto* mailbox = FindMailbox(cellId);
        if (!mailbox)
            return;

        mailbox->SetPostMessagesInFlight(false);

        int lastAcknowledgedMessageId = request.last_acknowledged_message_id();
        int acknowledgeCount = lastAcknowledgedMessageId - mailbox->GetFirstOutcomingMessageId() + 1;
        if (acknowledgeCount <= 0) {
            LOG_DEBUG_UNLESS(IsRecovery(), "No messages acknowledged (SrcCellId: %v, DstCellId: %v, FirstOutcomingMessageId: %v)",
                SelfCellId_,
                mailbox->GetCellId(),
                mailbox->GetFirstOutcomingMessageId());
            return;
        }

        auto& outcomingMessages = mailbox->OutcomingMessages();
        if (acknowledgeCount > outcomingMessages.size()) {
            LOG_ERROR_UNLESS(IsRecovery(), "Requested to acknowledge too many messages "
                "(SrcCellId: %v, DstCellId: %v, FirstOutcomingMessageId: %v, OutcomingMessageCount: %v, LastAcknowledgedMessageId: %v)",
                SelfCellId_,
                mailbox->GetCellId(),
                mailbox->GetFirstOutcomingMessageId(),
                outcomingMessages.size(),
                lastAcknowledgedMessageId);
            return;
        }

        outcomingMessages.erase(outcomingMessages.begin(), outcomingMessages.begin() + acknowledgeCount);

        mailbox->SetFirstOutcomingMessageId(mailbox->GetFirstOutcomingMessageId() + acknowledgeCount);
        LOG_DEBUG_UNLESS(IsRecovery(), "Messages acknowledged (SrcCellId: %v, DstCellId: %v, FirstOutcomingMessageId: %v)",
            SelfCellId_,
            mailbox->GetCellId(),
            mailbox->GetFirstOutcomingMessageId());

        if (IsLeader()) {
            MaybePostOutcomingMessages(mailbox);
        }
    }

    void HydraPostMessages(TCtxPostMessagesPtr context, const TReqPostMessages& request)
    {
        auto srcCellId = FromProto<TCellId>(request.src_cell_id());
        int firstMessageId = request.first_message_id();
        auto* mailbox = FindMailbox(srcCellId);
        if (!mailbox) {
            if (firstMessageId == 0) {
                mailbox = CreateMailbox(srcCellId);
            } else {
                auto error = TError("Mailbox does not exist; expecting message 0 but got %v",
                    firstMessageId);
                LOG_DEBUG_UNLESS(IsRecovery(), error, "Reliable incoming messages dropped (SrcCellId: %v, DstCellId: %v)",
                    srcCellId,
                    SelfCellId_);
                if (context) {
                    context->Reply(error);
                }
                return;
            }
        }

        HandleReliableIncomingMessages(mailbox, request);

        if (context) {
            auto* response = &context->Response();
            int lastAcknowledgedMessageId = mailbox->GetLastIncomingMessageId();
            response->set_last_acknowledged_message_id(lastAcknowledgedMessageId);
            context->SetResponseInfo("LastAcknowledgedMessageId: %v",
                lastAcknowledgedMessageId);
        }
    }

    void HydraSendMessages(TCtxSendMessagesPtr context, const TReqSendMessages& request)
    {
        auto srcCellId = FromProto<TCellId>(request.src_cell_id());
        auto* mailbox = FindMailbox(srcCellId);
        if (!mailbox) {
            auto error = TError("Mailbox %v does not exist", srcCellId);
            LOG_DEBUG_UNLESS(IsRecovery(), error, "Unreliable incoming messages dropped (SrcCellId: %v, DstCellId: %v)",
                srcCellId,
                SelfCellId_);
            if (context) {
                context->Reply(error);
            }
            return;
        }

        HandleUnreliableIncomingMessages(mailbox, request);

        if (context) {
            context->Reply();
        }
    }

    void HydraUnregisterMailbox(const TReqUnregisterMailbox& request)
    {
        auto cellId = FromProto<TCellId>(request.cell_id());
        auto* mailbox = FindMailbox(cellId);
        if (!mailbox)
            return;

        MailboxMap_.Remove(cellId);

        LOG_DEBUG_UNLESS(IsRecovery(), "Mailbox unregistered (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            cellId);
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

        auto proxy = std::make_unique<THiveServiceProxy>(channel);
        proxy->SetDefaultTimeout(Config_->RpcTimeout);
        return proxy;
    }


    void ReliablePostMessage(TMailbox* mailbox, TEncapsulatedMessage message)
    {
        // A typical mistake is to try sending a Hive message outside of a mutation.
        YCHECK(HasMutationContext());

        int messageId =
            mailbox->GetFirstOutcomingMessageId() +
            static_cast<int>(mailbox->OutcomingMessages().size());

        LOG_DEBUG_UNLESS(IsRecovery(), "Reliable outcoming message added (SrcCellId: %v, DstCellId: %v, MessageId: %v, MutationType: %v)",
            SelfCellId_,
            mailbox->GetCellId(),
            messageId,
            message.type());

        AnnotateWithTraceContext(&message);
        mailbox->OutcomingMessages().push_back(std::move(message));

        MaybePostOutcomingMessages(mailbox);
    }

    void UnreliablePostMessage(TMailbox* mailbox, TEncapsulatedMessage message)
    {
        if (!mailbox->GetConnected()) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Unreliable outcoming message dropped since mailbox is not connected (SrcCellId: %v, DstCellId: %v, MutationType: %v)",
                SelfCellId_,
                mailbox->GetCellId(),
                message.type());
        }

        auto proxy = FindHiveProxy(mailbox);
        if (!proxy) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Unreliable outcoming message dropped since no channel exists (SrcCellId: %v, DstCellId: %v, MutationType: %v)",
                SelfCellId_,
                mailbox->GetCellId(),
                message.type());
        }

        AnnotateWithTraceContext(&message);

        auto req = proxy->SendMessages();
        ToProto(req->mutable_src_cell_id(), SelfCellId_);
        *req->add_messages() = message;

        LOG_DEBUG("Sending unreliable outcoming message (SrcCellId: %v, DstCellId: %v, MutationType: %v)",
            SelfCellId_,
            mailbox->GetCellId(),
            message.type());

        req->Invoke().Subscribe(
            BIND(&TImpl::OnSendMessagesResponse, MakeStrong(this), mailbox->GetCellId())
                .Via(EpochAutomatonInvoker_));
    }


    void SetMailboxConnected(TMailbox* mailbox)
    {
        if (mailbox->GetConnected())
            return;

        mailbox->SetConnected(true);
        YCHECK(mailbox->SyncRequests().empty());
        YCHECK(!mailbox->GetPostMessagesInFlight());

        LOG_INFO("Mailbox connected (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            mailbox->GetCellId());
    }

    void SetMailboxDisconnected(TMailbox* mailbox)
    {
        if (!mailbox->GetConnected())
            return;

        mailbox->SetConnected(false);
        mailbox->SyncRequests().clear();
        mailbox->SetPostMessagesInFlight(false);

        LOG_INFO("Mailbox disconnected (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            mailbox->GetCellId());
    }


    void SchedulePeriodicPing(TMailbox* mailbox)
    {
        TDelayedExecutor::Submit(
            BIND(&TImpl::OnPeriodicPingTick, MakeWeak(this), mailbox->GetCellId())
                .Via(EpochAutomatonInvoker_),
            Config_->PingPeriod);
    }

    void OnPeriodicPingTick(const TCellId& cellId)
    {
        auto* mailbox = FindMailbox(cellId);
        if (!mailbox)
            return;

        SendPeriodicPing(mailbox);
    }

    void SendPeriodicPing(TMailbox* mailbox)
    {
        const auto& cellId = mailbox->GetCellId();

        if (CellDirectory_->IsCellUnregistered(cellId)) {
            TReqUnregisterMailbox req;
            ToProto(req.mutable_cell_id(), cellId);

            CreateUnregisterMailboxMutation(req)
                ->Commit()
                .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TMutationResponse>& error) {
                    if (!error.IsOK()) {
                        LOG_ERROR(error, "Error committing mailbox unregistration mutation");
                    }
                }));

            return;
        }

        if (mailbox->GetConnected()) {
            SchedulePeriodicPing(mailbox);
            return;
        }

        auto proxy = FindHiveProxy(mailbox);
        if (!proxy) {
            // Let's register a dummy descriptor so as to ask about it during the next sync.
            NHive::TCellDescriptor descriptor;
            descriptor.CellId = cellId;
            CellDirectory_->ReconfigureCell(descriptor);
            SchedulePeriodicPing(mailbox);
            return;
        }

        LOG_DEBUG("Sending periodic ping (SrcCellId: %v, DstCellId: %v)",
            SelfCellId_,
            mailbox->GetCellId());

        auto req = proxy->Ping();
        ToProto(req->mutable_src_cell_id(), SelfCellId_);

        req->Invoke().Subscribe(
            BIND(&TImpl::OnPeriodicPingResponse, MakeStrong(this), mailbox->GetCellId())
                .Via(EpochAutomatonInvoker_));
    }

    void OnPeriodicPingResponse(const TCellId& cellId, const THiveServiceProxy::TErrorOrRspPingPtr& rspOrError)
    {
        auto* mailbox = FindMailbox(cellId);
        if (!mailbox)
            return;

        SchedulePeriodicPing(mailbox);

        if (!rspOrError.IsOK()) {
            LOG_DEBUG(rspOrError, "Periodic ping failed (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                mailbox->GetCellId());
            return;
        }

        const auto& rsp = rspOrError.Value();
        int lastIncomingMessageId = rsp->last_incoming_message_id();
        int lastOutcomingMessageId = rsp->last_outcoming_message_id();

        LOG_DEBUG("Periodic ping succeeded (SrcCellId: %v, DstCellId: %v, LastIncomingMessageId: %v, LastOutcomingMessageId: %v)",
            SelfCellId_,
            mailbox->GetCellId(),
            lastIncomingMessageId,
            lastOutcomingMessageId);

        SetMailboxConnected(mailbox);
        HandleAcknowledgedMessages(mailbox, lastIncomingMessageId);
        MaybePostOutcomingMessages(mailbox);
    }


    TFuture<void> OnSyncPingResponse(const TCellId& cellId, const THiveServiceProxy::TErrorOrRspPingPtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            THROW_ERROR_EXCEPTION("Sync ping to cell %v has failed",
                cellId)
                    << rspOrError;
        }

        auto* mailbox = GetMailboxOrThrow(cellId);
        if (!mailbox->GetConnected()) {
            THROW_ERROR_EXCEPTION("Mailbox %v is not connected",
                cellId);
        }

        const auto& rsp = rspOrError.Value();
        int messageId = rsp->last_outcoming_message_id();

        if (messageId <= mailbox->GetLastIncomingMessageId()) {
            LOG_DEBUG("Already synchronized with another instance (SrcCellId: %v, DstCellId: %v, NeededMessageId: %v, CurrentMessageId: %v)",
                SelfCellId_,
                mailbox->GetCellId(),
                messageId,
                mailbox->GetLastIncomingMessageId());
            return VoidFuture;
        }

        LOG_DEBUG("Waiting for synchronization with another instance (SrcCellId: %v, DstCellId: %v, NeededMessageId: %v, CurrentMessageId: %v)",
            SelfCellId_,
            cellId,
            messageId,
            mailbox->GetLastIncomingMessageId());

        return RegisterSyncRequest(mailbox, messageId);
    }

    TFuture<void> RegisterSyncRequest(TMailbox* mailbox, int messageId)
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
            int messageId = it->first;
            if (messageId > mailbox->GetLastIncomingMessageId())
                break;

            auto& request = it->second;

            LOG_DEBUG("Synchronization complete (SrcCellId: %v, DstCellId: %v, MessageId: %v)",
                SelfCellId_,
                mailbox->GetCellId(),
                messageId);

            request.Promise.Set();
            syncRequests.erase(it);
        }
    }


    void MaybePostOutcomingMessages(TMailbox* mailbox)
    {
        if (!IsLeader())
            return;

        if (!mailbox->GetConnected())
            return;

        if (mailbox->GetPostMessagesInFlight())
            return;

        if (mailbox->OutcomingMessages().empty())
            return;

        auto proxy = FindHiveProxy(mailbox);
        if (!proxy)
            return;

        int firstMessageId = mailbox->GetFirstOutcomingMessageId();
        int messageCount = mailbox->OutcomingMessages().size();

        auto req = proxy->PostMessages();
        ToProto(req->mutable_src_cell_id(), SelfCellId_);
        req->set_first_message_id(firstMessageId);
        for (const auto& message : mailbox->OutcomingMessages()) {
            *req->add_messages() = message;
        }

        mailbox->SetPostMessagesInFlight(true);

        LOG_DEBUG("Posting reliable outcoming messages (SrcCellId: %v, DstCellId: %v, MessageIds: %v-%v)",
            SelfCellId_,
            mailbox->GetCellId(),
            firstMessageId,
            firstMessageId + messageCount - 1);

        req->Invoke().Subscribe(
            BIND(&TImpl::OnPostMessagesResponse, MakeStrong(this), mailbox->GetCellId())
                .Via(EpochAutomatonInvoker_));
    }

    void OnPostMessagesResponse(const TCellId& cellId, const THiveServiceProxy::TErrorOrRspPostMessagesPtr& rspOrError)
    {
        auto* mailbox = FindMailbox(cellId);
        if (!mailbox)
            return;

        if (!rspOrError.IsOK()) {
            LOG_DEBUG(rspOrError, "Failed to post reliable outcoming messages (SrcCellId: %v, DstCellId: %v)",
                SelfCellId_,
                mailbox->GetCellId());
            SetMailboxDisconnected(mailbox);
            return;
        }

        const auto& rsp = rspOrError.Value();
        int lastAcknowledgedMessageId = rsp->last_acknowledged_message_id();
        LOG_DEBUG("Outcoming reliable messages posted successfully (SrcCellId: %v, DstCellId: %v, LastAcknowledgedMessageId: %v)",
            SelfCellId_,
            mailbox->GetCellId(),
            lastAcknowledgedMessageId);

        HandleAcknowledgedMessages(mailbox, lastAcknowledgedMessageId);
    }

    void OnSendMessagesResponse(const TCellId& cellId, const THiveServiceProxy::TErrorOrRspSendMessagesPtr& rspOrError)
    {
        auto* mailbox = FindMailbox(cellId);
        if (!mailbox)
            return;

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


    TMutationPtr CreateAcknowledgeMessagesMutation(const TReqAcknowledgeMessages& req)
    {
        return CreateMutation(
            HydraManager_,
            req,
            this,
            &TImpl::HydraAcknowledgeMessages);
    }

    TMutationPtr CreatePostMessagesMutation(TCtxPostMessagesPtr context)
    {
        return CreateMutation(HydraManager_)
            ->SetRequestData(context->GetRequestBody(), context->Request().GetTypeName())
            ->SetAction(BIND(&TImpl::HydraPostMessages, MakeStrong(this), context, ConstRef(context->Request())));
    }

    TMutationPtr CreateSendMessagesMutation(TCtxSendMessagesPtr context)
    {
        return CreateMutation(HydraManager_)
            ->SetRequestData(context->GetRequestBody(), context->Request().GetTypeName())
            ->SetAction(BIND(&TImpl::HydraSendMessages, MakeStrong(this), context, ConstRef(context->Request())));
    }

    TMutationPtr CreateUnregisterMailboxMutation(const TReqUnregisterMailbox& req)
    {
        return CreateMutation(
            HydraManager_,
            req,
            this,
            &TImpl::HydraUnregisterMailbox);
    }


    void HandleAcknowledgedMessages(TMailbox* mailbox, int lastAcknowledgedMessageId)
    {
        if (lastAcknowledgedMessageId < mailbox->GetFirstOutcomingMessageId())
            return;

        TReqAcknowledgeMessages req;
        ToProto(req.mutable_cell_id(), mailbox->GetCellId());
        req.set_last_acknowledged_message_id(lastAcknowledgedMessageId);

        CreateAcknowledgeMessagesMutation(req)
            ->Commit()
            .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TMutationResponse>& error) {
                if (!error.IsOK()) {
                    LOG_ERROR(error, "Error committing message acknowledgment mutation");
                }
            }));
    }

    void HandleReliableIncomingMessages(TMailbox* mailbox, const TReqPostMessages& req)
    {
        for (int index = 0; index < req.messages_size(); ++index) {
            int messageId = req.first_message_id() + index;
            HandleReliableIncomingMessage(mailbox, messageId, req.messages(index));
        }
    }

    void HandleReliableIncomingMessage(TMailbox* mailbox, int messageId, const TEncapsulatedMessage& incomingMessage)
    {
        if (messageId <= mailbox->GetLastIncomingMessageId()) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Dropping obsolete incoming message (SrcCellId: %v, DstCellId: %v, MessageId: %v)",
                mailbox->GetCellId(),
                SelfCellId_,
                messageId);
            return;
        }

        auto& incomingMessages = mailbox->IncomingMessages();
        if (!incomingMessages.insert(std::make_pair(messageId, incomingMessage)).second) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Dropping duplicate incoming message (SrcCellId: %v, DstCellId: %v, MessageId: %v)",
                mailbox->GetCellId(),
                SelfCellId_,
                messageId);
            return;
        }

        bool consumed = false;
        while (!incomingMessages.empty()) {
            const auto& frontPair = *incomingMessages.begin();
            int frontMessageId = frontPair.first;
            const auto& frontMessage = frontPair.second;
            if (frontMessageId != mailbox->GetLastIncomingMessageId() + 1)
                break;

            LOG_DEBUG_UNLESS(IsRecovery(), "Consuming reliable incoming message (SrcCellId: %v, DstCellId: %v, MessageId: %v, MutationType: %v)",
                mailbox->GetCellId(),
                SelfCellId_,
                frontMessageId,
                frontMessage.type());

            TMutationRequest request;
            request.Type = frontMessage.type();
            request.Data = TSharedRef::FromString(frontMessage.data());

            auto traceContext = GetTraceContext(frontMessage);
            TTraceContextGuard traceContextGuard(traceContext);

            ApplyMessage(frontMessage);

            YCHECK(incomingMessages.erase(frontMessageId) == 1);
            mailbox->SetLastIncomingMessageId(frontMessageId);
            consumed = true;

            FlushSyncRequests(mailbox);
        }

        if (!consumed) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Keeping an out-of-order incoming message (SrcCellId: %v, DstCellId: %v, MessageId: %v)",
                mailbox->GetCellId(),
                SelfCellId_,
                messageId);
        }
    }

    void HandleUnreliableIncomingMessages(TMailbox* mailbox, const TReqSendMessages& req)
    {
        for (const auto& message : req.messages()) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Consuming unreliable incoming message (SrcCellId: %v, DstCellId: %v, MutationType: %v)",
                mailbox->GetCellId(),
                SelfCellId_,
                message.type());
            ApplyMessage(message);
        }
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
            TMutationContext context(GetCurrentMutationContext(), request);
            TMutationContextGuard contextGuard(&context);
            static_cast<IAutomaton*>(Automaton_)->ApplyMutation(&context);
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
            TAddressResolver::Get()->GetLocalHostName());

        message->set_trace_id(traceContext.GetTraceId());
        message->set_span_id(traceContext.GetSpanId());
        message->set_parent_span_id(traceContext.GetParentSpanId());
    }


    virtual void OnLeaderActive() override
    {
        TCompositeAutomatonPart::OnLeaderActive();

        for (const auto& pair : MailboxMap_) {
            auto* mailbox = pair.second;
            SetMailboxDisconnected(mailbox);
            SendPeriodicPing(mailbox);
        }
    }


    virtual bool ValidateSnapshotVersion(int version) override
    {
        return version == 1;
    }

    virtual int GetCurrentSnapshotVersion() override
    {
        return 1;
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

};

DEFINE_ENTITY_MAP_ACCESSORS(THiveManager::TImpl, Mailbox, TMailbox, TCellId, MailboxMap_)

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

THiveManager::~THiveManager()
{ }

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

void THiveManager::RemoveMailbox(const TCellId& cellId)
{
    Impl_->RemoveMailbox(cellId);
}

void THiveManager::PostMessage(TMailbox* mailbox, const TEncapsulatedMessage& message, bool reliable)
{
    Impl_->PostMessage(mailbox, message, reliable);
}

void THiveManager::PostMessage(TMailbox* mailbox, const ::google::protobuf::MessageLite& message, bool reliable)
{
    Impl_->PostMessage(mailbox, message, reliable);
}

TFuture<void> THiveManager::SyncWith(const TCellId& cellId)
{
    return Impl_->SyncWith(cellId);
}

void THiveManager::BuildOrchidYson(IYsonConsumer* consumer)
{
    Impl_->BuildOrchidYson(consumer);
}

DELEGATE_ENTITY_MAP_ACCESSORS(THiveManager, Mailbox, TMailbox, TCellId, *Impl_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
