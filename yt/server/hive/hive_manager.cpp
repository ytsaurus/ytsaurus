#include "stdafx.h"
#include "hive_manager.h"
#include "config.h"
#include "hive_service_proxy.h"
#include "mailbox.h"
#include "private.h"

#include <core/misc/address.h>

#include <core/rpc/service_detail.h>
#include <core/rpc/server.h>

#include <core/concurrency/delayed_executor.h>

#include <core/ytree/fluent.h>

#include <core/tracing/trace_context.h>

#include <core/rpc/rpc.pb.h>

#include <ytlib/hydra/peer_channel.h>
#include <ytlib/hydra/config.h>

#include <ytlib/hive/cell_directory.h>

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

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HiveLogger;

static auto HiveTracingService = Stroka("HiveManager");
static auto ClientHostAnnotation = Stroka("client_host");

////////////////////////////////////////////////////////////////////////////////

class THiveManager::TImpl
    : public THydraServiceBase
    , public TCompositeAutomatonPart
{
public:
    TImpl(
        const TCellGuid& cellGuid,
        THiveManagerConfigPtr config,
        TCellDirectoryPtr cellDirectory,
        IInvokerPtr automatonInvoker,
        IHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr automaton)
        : THydraServiceBase(
            hydraManager,
            automatonInvoker,
            TServiceId(THiveServiceProxy::GetServiceName(), cellGuid),
            HiveLogger.GetCategory())
        , TCompositeAutomatonPart(
            hydraManager,
            automaton)
        , SelfCellGuid_(cellGuid)
        , Config_(config)
        , CellDirectory_(cellDirectory)
    {
        Automaton->RegisterPart(this);

        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(Ping));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(PostMessages));

        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraAcknowledgeMessages, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraPostMessages, Unretained(this), nullptr));

        RegisterLoader(
            "HiveManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "HiveManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESerializationPriority::Keys,
            "HiveManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESerializationPriority::Values,
            "HiveManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));
    }

    IServicePtr GetRpcService()
    {
        return this;
    }

    const TCellGuid& GetSelfCellGuid() const
    {
        return SelfCellGuid_;
    }


    TMailbox* CreateMailbox(const TCellGuid& cellGuid)
    {
        auto* mailbox = new TMailbox(cellGuid);
        MailboxMap_.Insert(cellGuid, mailbox);
        
        if (IsLeader()) {
            SendPing(mailbox);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Mailbox created (SrcCellGuid: %v, DstCellGuid: %v)",
            SelfCellGuid_,
            mailbox->GetCellGuid());
        return mailbox;
    }
    
    TMailbox* GetOrCreateMailbox(const TCellGuid& cellGuid)
    {
        auto* mailbox = MailboxMap_.Find(cellGuid);
        if (!mailbox) {
            mailbox = CreateMailbox(cellGuid);
        }
        return mailbox;
    }

    TMailbox* GetMailboxOrThrow(const TCellGuid& cellGuid)
    {
        auto* mailbox = FindMailbox(cellGuid);
        if (!mailbox) {
            THROW_ERROR_EXCEPTION("No such mailbox %v",
                cellGuid);
        }
        return mailbox;
    }

    void RemoveMailbox(const TCellGuid& cellGuid)
    {
        MailboxMap_.Remove(cellGuid);
        LOG_INFO_UNLESS(IsRecovery(), "Mailbox removed (SrcCellGuid: %v, DstCellGuid: %v)",
            SelfCellGuid_,
            cellGuid);
    }

    void PostMessage(TMailbox* mailbox, const TEncapsulatedMessage& message)
    {
        // A typical mistake is to try sending a Hive message outside of a mutation.
        YCHECK(HydraManager->IsMutating());

        int messageId =
            mailbox->GetFirstOutcomingMessageId() +
            static_cast<int>(mailbox->OutcomingMessages().size());

        auto tracedMessage = message;
        auto traceContext = CreateChildTraceContext();
        if (traceContext.IsEnabled()) {
            TRACE_ANNOTATION(
                traceContext,
                HiveTracingService,
                message.type(),
                ClientSendAnnotation);

            TRACE_ANNOTATION(
                traceContext,
                ClientHostAnnotation,
                TAddressResolver::Get()->GetLocalHostName());

            tracedMessage.set_trace_id(traceContext.GetTraceId());
            tracedMessage.set_span_id(traceContext.GetSpanId());
            tracedMessage.set_parent_span_id(traceContext.GetParentSpanId());
        }

        mailbox->OutcomingMessages().push_back(tracedMessage);

        LOG_DEBUG_UNLESS(IsRecovery(), "Outcoming message added (SrcCellGuid: %v, DstCellGuid: %v, MessageId: %v)",
            SelfCellGuid_,
            mailbox->GetCellGuid(),
            messageId);

        MaybePostOutcomingMessages(mailbox);
    }

    void PostMessage(TMailbox* mailbox, const ::google::protobuf::MessageLite& message)
    {
        TEncapsulatedMessage encapsulatedMessage;
        encapsulatedMessage.set_type(message.GetTypeName());

        TSharedRef serializedMessage;
        YCHECK(SerializeToProtoWithEnvelope(message, &serializedMessage));
        encapsulatedMessage.set_data(ToString(serializedMessage));

        PostMessage(mailbox, encapsulatedMessage);
    }

    void BuildOrchidYson(IYsonConsumer* consumer)
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("mailboxes").DoMapFor(MailboxMap_, [&] (TFluentMap fluent, const std::pair<TCellGuid, TMailbox*>& pair) {
                    auto* mailbox = pair.second;
                    fluent
                        .Item(ToString(mailbox->GetCellGuid())).BeginMap()
                            .Item("first_outcoming_message_id").Value(mailbox->GetFirstOutcomingMessageId())
                            .Item("last_incoming_message_id").Value(mailbox->GetLastIncomingMessageId())
                            .Item("in_flight_message_count").Value(mailbox->GetInFlightMessageCount())
                            .Item("outcoming_message_count").Value(mailbox->OutcomingMessages().size())
                            .Item("incoming_message_ids").BeginList().DoFor(mailbox->IncomingMessages(), [&] (TFluentList fluent, const TMailbox::TIncomingMessageMap::value_type& pair) {
                                fluent
                                    .Item().Value(pair.first);
                            })
                            .EndList()
                        .EndMap();
                })
            .EndMap();
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Mailbox, TMailbox, TCellGuid);

private:
    TCellGuid SelfCellGuid_;
    THiveManagerConfigPtr Config_;
    TCellDirectoryPtr CellDirectory_;

    TEntityMap<TCellGuid, TMailbox> MailboxMap_;
    

    // RPC handlers.

    DECLARE_RPC_SERVICE_METHOD(NProto, Ping)
    {
        auto srcCellGuid = FromProto<TCellGuid>(request->src_cell_guid());

        context->SetRequestInfo("SrcCellGuid: %v, DstCellGuid: %v",
            srcCellGuid,
            SelfCellGuid_);

        auto* mailbox = FindMailbox(srcCellGuid);
        int lastIncomingMessageId = mailbox ? mailbox->GetLastIncomingMessageId() : -1;

        response->set_last_incoming_message_id(lastIncomingMessageId);

        context->SetResponseInfo("LastIncomingMessageId: %v",
            lastIncomingMessageId);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, PostMessages)
    {
        auto srcCellGuid = FromProto<TCellGuid>(request->src_cell_guid());
        int firstMessageId = request->first_message_id();

        context->SetRequestInfo("SrcCellGuid: %v, DstCellGuid: %v, MessageIds: %v-%v",
            srcCellGuid,
            SelfCellGuid_,
            firstMessageId,
            firstMessageId + request->messages_size() - 1);
        
        CreatePostMessagesMutation(context)
            ->OnSuccess(CreateRpcSuccessHandler(context))
            ->Commit();
    }


    // Hydra handlers.

    void HydraAcknowledgeMessages(const TReqAcknowledgeMessages& request)
    {
        auto cellGuid = FromProto<TCellGuid>(request.cell_guid());
        auto* mailbox = FindMailbox(cellGuid);
        if (!mailbox)
            return;

        int lastIncomingMessageId = request.last_incoming_message_id();
        int trimCount = lastIncomingMessageId - mailbox->GetFirstOutcomingMessageId() + 1;
        YCHECK(trimCount >= 0);
        if (trimCount == 0)
            return;

        auto& outcomingMessages = mailbox->OutcomingMessages();
        std::move(
            outcomingMessages.begin() + trimCount,
            outcomingMessages.end(),
            outcomingMessages.begin());
        outcomingMessages.resize(outcomingMessages.size() - trimCount);

        mailbox->SetFirstOutcomingMessageId(mailbox->GetFirstOutcomingMessageId() + trimCount);
        LOG_DEBUG_UNLESS(IsRecovery(), "Messages acknowledged (SrcCellGuid: %v, DstCellGuid: %v, FirstOutcomingMessageId: %v)",
            SelfCellGuid_,
            mailbox->GetCellGuid(),
            mailbox->GetFirstOutcomingMessageId());

        if (IsLeader()) {
            mailbox->SetInFlightMessageCount(std::max(0, mailbox->GetInFlightMessageCount() - trimCount));
        }
    }

    void HydraPostMessages(TCtxPostMessagesPtr context, const TReqPostMessages& request)
    {
        auto srcCellGuid = FromProto<TCellGuid>(request.src_cell_guid());
        auto* mailbox = GetOrCreateMailbox(srcCellGuid);

        HandleIncomingMessages(mailbox, request);

        if (context) {
            auto* response = &context->Response();
            int lastIncomingMessageId = mailbox->GetLastIncomingMessageId();
            response->set_last_incoming_message_id(lastIncomingMessageId);
            context->SetResponseInfo("LastIncomingMessageId: %v",
                lastIncomingMessageId);
        }
    }


    IChannelPtr GetMailboxChannel(TMailbox* mailbox)
    {
        return CellDirectory_->FindChannel(mailbox->GetCellGuid());
    }


    void SetMailboxConnected(TMailbox* mailbox)
    {
        if (mailbox->GetConnected())
            return;

        mailbox->SetConnected(true);
        LOG_INFO("Mailbox connected (SrcCellGuid: %v, DstCellGuid: %v)",
            SelfCellGuid_,
            mailbox->GetCellGuid());
    }

    void SetMailboxDisconnected(TMailbox* mailbox)
    {
        if (!mailbox->GetConnected())
            return;

        mailbox->SetConnected(false);
        mailbox->SetInFlightMessageCount(0);
        LOG_INFO("Mailbox disconnected (SrcCellGuid: %v, DstCellGuid: %v)",
            SelfCellGuid_,
            mailbox->GetCellGuid());
    }


    void SchedulePing(TMailbox* mailbox)
    {
        TDelayedExecutor::Submit(
            BIND(&TImpl::OnPingTick, MakeWeak(this), mailbox->GetCellGuid())
                .Via(EpochAutomatonInvoker_),
            Config_->PingPeriod);
    }

    void OnPingTick(const TCellGuid& cellGuid)
    {
        auto* mailbox = FindMailbox(cellGuid);
        if (!mailbox)
            return;

        SendPing(mailbox);
    }

    void SendPing(TMailbox* mailbox)
    {
        if (mailbox->GetConnected()) {
            SchedulePing(mailbox);
            return;
        }

        auto channel = GetMailboxChannel(mailbox);
        if (!channel) {
            SchedulePing(mailbox);
            return;
        }

        LOG_DEBUG("Sending ping (SrcCellGuid: %v, DstCellGuid: %v)",
            SelfCellGuid_,
            mailbox->GetCellGuid());

        THiveServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(Config_->RpcTimeout);
        
        auto req = proxy.Ping();
        ToProto(req->mutable_src_cell_guid(), SelfCellGuid_);
        req->Invoke().Subscribe(
            BIND(&TImpl::OnPingResponse, MakeStrong(this), mailbox->GetCellGuid())
                .Via(EpochAutomatonInvoker_));
    }

    void OnPingResponse(const TCellGuid& cellGuid, THiveServiceProxy::TRspPingPtr rsp)
    {
        auto* mailbox = FindMailbox(cellGuid);
        if (!mailbox)
            return;

        if (!rsp->IsOK()) {
            LOG_DEBUG(*rsp, "Ping failed (SrcCellGuid: %v, DstCellGuid: %v)",
                SelfCellGuid_,
                mailbox->GetCellGuid());
            SchedulePing(mailbox);
            return;
        }

        int lastIncomingMessageId = rsp->last_incoming_message_id();
        LOG_DEBUG("Ping succeeded (SrcCellGuid: %v, DstCellGuid: %v, LastReceivedMessagId: %v)",
            SelfCellGuid_,
            mailbox->GetCellGuid(),
            lastIncomingMessageId);

        SetMailboxConnected(mailbox);
        HandleAcknowledgedMessages(mailbox, lastIncomingMessageId);
        MaybePostOutcomingMessages(mailbox);
        SchedulePing(mailbox);
    }


    void MaybePostOutcomingMessages(TMailbox* mailbox)
    {
        if (!IsLeader())
            return;

        if (!mailbox->GetConnected())
            return;

        auto channel = GetMailboxChannel(mailbox);
        if (!channel)
            return;

        if (mailbox->OutcomingMessages().size() <= mailbox->GetInFlightMessageCount())
            return;

        THiveServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(Config_->RpcTimeout);

        auto req = proxy.PostMessages();
        ToProto(req->mutable_src_cell_guid(), SelfCellGuid_);
        int firstMessageId = mailbox->GetFirstOutcomingMessageId() + mailbox->GetInFlightMessageCount();
        int messageCount = mailbox->OutcomingMessages().size() - mailbox->GetInFlightMessageCount();
        req->set_first_message_id(firstMessageId);
        for (auto it = mailbox->OutcomingMessages().begin() + mailbox->GetInFlightMessageCount();
             it != mailbox->OutcomingMessages().end();
             ++it)
        {
            *req->add_messages() = *it;
        }

        mailbox->SetInFlightMessageCount(mailbox->GetInFlightMessageCount() + messageCount);

        LOG_DEBUG("Posting outcoming messages (SrcCellGuid: %v, DstCellGuid: %v, MessageIds: %v-%v)",
            SelfCellGuid_,
            mailbox->GetCellGuid(),
            firstMessageId,
            firstMessageId + messageCount - 1);

        req->Invoke().Subscribe(
            BIND(&TImpl::OnPostMessagesResponse, MakeStrong(this), mailbox->GetCellGuid())
                .Via(EpochAutomatonInvoker_));
    }

    void OnPostMessagesResponse(const TCellGuid& cellGuid, THiveServiceProxy::TRspPostMessagesPtr rsp)
    {
        auto* mailbox = FindMailbox(cellGuid);
        if (!mailbox)
            return;

        if (!rsp->IsOK()) {
            LOG_DEBUG(*rsp, "Failed to post outcoming messages (SrcCellGuid: %v, DstCellGuid: %v)",
                SelfCellGuid_,
                mailbox->GetCellGuid());
            SetMailboxDisconnected(mailbox);
            return;
        }

        int lastIncomingMessageId = rsp->last_incoming_message_id();
        LOG_DEBUG("Outcoming messages posted successfully (SrcCellGuid: %v, DstCellGuid: %v, LastIncomingMessageId: %v)",
            SelfCellGuid_,
            mailbox->GetCellGuid(),
            lastIncomingMessageId);

        HandleAcknowledgedMessages(mailbox, lastIncomingMessageId);
    }


    TMutationPtr CreateAcknowledgeMessagesMutation(const TReqAcknowledgeMessages& req)
    {
        return CreateMutation(
            HydraManager,
            req,
            this,
            &TImpl::HydraAcknowledgeMessages);
    }

    TMutationPtr CreatePostMessagesMutation(TCtxPostMessagesPtr context)
    {
        return CreateMutation(HydraManager)
            ->SetRequestData(context->GetRequestBody(), context->Request().GetTypeName())
            ->SetAction(BIND(&TImpl::HydraPostMessages, MakeStrong(this), context, ConstRef(context->Request())));
    }


    void HandleAcknowledgedMessages(TMailbox* mailbox, int lastIncomingMessageId)
    {
        if (lastIncomingMessageId < mailbox->GetFirstOutcomingMessageId())
            return;

        TReqAcknowledgeMessages req;
        ToProto(req.mutable_cell_guid(), mailbox->GetCellGuid());
        req.set_last_incoming_message_id(lastIncomingMessageId);
        CreateAcknowledgeMessagesMutation(req)
            ->Commit();
    }

    void HandleIncomingMessages(TMailbox* mailbox, const TReqPostMessages& req)
    {
        for (int index = 0; index < req.messages_size(); ++index) {
            int messageId = req.first_message_id() + index;
            HandleIncomingMessage(mailbox, messageId, req.messages(index));
        }
    }

    void HandleIncomingMessage(TMailbox* mailbox, int messageId, const TEncapsulatedMessage& message)
    {
        if (messageId <= mailbox->GetLastIncomingMessageId()) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Dropping an obsolete incoming message (SrcCellGuid: %v, DstCellGuid: %v, MessageId: %v)",
                mailbox->GetCellGuid(),
                SelfCellGuid_,
                messageId);
        } else {
            auto& incomingMessages = mailbox->IncomingMessages();
            YCHECK(incomingMessages.insert(std::make_pair(messageId, message)).second);

            bool consumed = false;
            while (!incomingMessages.empty()) {
                const auto& frontPair = *incomingMessages.begin();
                int frontMessageId = frontPair.first;
                const auto& frontMessage = frontPair.second;
                if (frontMessageId != mailbox->GetLastIncomingMessageId() + 1)
                    break;

                LOG_DEBUG_UNLESS(IsRecovery(), "Consuming incoming message (SrcCellGuid: %v, DstCellGuid: %v, MessageId: %v)",
                    mailbox->GetCellGuid(),
                    SelfCellGuid_,
                    frontMessageId);

                TMutationRequest request;
                request.Type = frontMessage.type();
                request.Data = TSharedRef::FromString(frontMessage.data());

                auto traceContext = GetTraceContext(message);
                TTraceContextGuard traceContextGuard(traceContext);

                TRACE_ANNOTATION(
                    traceContext,
                    HiveTracingService,
                    request.Type,
                    ServerReceiveAnnotation);

                YCHECK(incomingMessages.erase(frontMessageId) == 1);
                mailbox->SetLastIncomingMessageId(frontMessageId);
                consumed = true;

                TMutationContext context(HydraManager->GetMutationContext(), request);
                static_cast<IAutomaton*>(Automaton)->ApplyMutation(&context);

                TRACE_ANNOTATION(
                    traceContext,
                    HiveTracingService,
                    request.Type,
                    ServerSendAnnotation);
            }

            if (!consumed) {
                LOG_DEBUG_UNLESS(IsRecovery(), "Keeping an out-of-order incoming message (SrcCellGuid: %v, DstCellGuid: %v, MessageId: %v)",
                    mailbox->GetCellGuid(),
                    SelfCellGuid_,
                    messageId);
            }
        }
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


    virtual void OnLeaderActive() override
    {
        for (const auto& pair : MailboxMap_) {
            auto* mailbox = pair.second;
            mailbox->SetInFlightMessageCount(0);
            mailbox->SetConnected(false);
            SendPing(mailbox);
        }
    }

    virtual void OnStopLeading() override
    {
        for (const auto& pair : MailboxMap_) {
            auto* mailbox = pair.second;
            mailbox->SetInFlightMessageCount(0);
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

DEFINE_ENTITY_MAP_ACCESSORS(THiveManager::TImpl, Mailbox, TMailbox, TCellGuid, MailboxMap_)

////////////////////////////////////////////////////////////////////////////////

THiveManager::THiveManager(
    const TCellGuid& selfCellGuid,
    THiveManagerConfigPtr config,
    TCellDirectoryPtr cellDirectory,
    IInvokerPtr automatonInvoker,
    IHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton)
    : Impl_(New<TImpl>(
        selfCellGuid,
        config,
        cellDirectory,
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

const TCellGuid& THiveManager::GetSelfCellGuid() const
{
    return Impl_->GetSelfCellGuid();
}

TMailbox* THiveManager::CreateMailbox(const TCellGuid& cellGuid)
{
    return Impl_->CreateMailbox(cellGuid);
}

TMailbox* THiveManager::GetOrCreateMailbox(const TCellGuid& cellGuid)
{
    return Impl_->GetOrCreateMailbox(cellGuid);
}

TMailbox* THiveManager::GetMailboxOrThrow(const TCellGuid& cellGuid)
{
    return Impl_->GetMailboxOrThrow(cellGuid);
}

void THiveManager::RemoveMailbox(const TCellGuid& cellGuid)
{
    Impl_->RemoveMailbox(cellGuid);
}

void THiveManager::PostMessage(TMailbox* mailbox, const TEncapsulatedMessage& message)
{
    Impl_->PostMessage(mailbox, message);
}

void THiveManager::PostMessage(TMailbox* mailbox, const ::google::protobuf::MessageLite& message)
{
    Impl_->PostMessage(mailbox, message);
}

void THiveManager::BuildOrchidYson(IYsonConsumer* consumer)
{
    Impl_->BuildOrchidYson(consumer);
}

DELEGATE_ENTITY_MAP_ACCESSORS(THiveManager, Mailbox, TMailbox, TCellGuid, *Impl_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
