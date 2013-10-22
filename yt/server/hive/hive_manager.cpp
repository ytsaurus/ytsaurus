#include "stdafx.h"
#include "hive_manager.h"
#include "cell_directory.h"
#include "config.h"
#include "hive_service_proxy.h"
#include "mailbox.h"
#include "private.h"

#include <core/rpc/service_detail.h>
#include <core/rpc/server.h>

#include <core/concurrency/delayed_executor.h>

#include <ytlib/hydra/peer_channel.h>
#include <ytlib/hydra/config.h>
#include <ytlib/hydra/rpc_helpers.h>

#include <server/election/election_manager.h>

#include <server/hydra/composite_automaton.h>
#include <server/hydra/hydra_manager.h>
#include <server/hydra/mutation_context.h>
#include <server/hydra/mutation.h>

#include <server/hive/hive_manager.pb.h>

namespace NYT {
namespace NHive {

using namespace NRpc;
using namespace NHydra;
using namespace NHydra::NProto;
using namespace NConcurrency;
using namespace NHive::NProto;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = HiveLogger;

////////////////////////////////////////////////////////////////////////////////

class THiveManager::TImpl
    : public TServiceBase
    , public TCompositeAutomatonPart
{
public:
    TImpl(
        const TCellGuid& cellGuid,
        THiveManagerConfigPtr config,
        TCellDirectoryPtr cellRegistry,
        IInvokerPtr automatonInvoker,
        NRpc::IRpcServerPtr rpcServer,
        IHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr automaton)
        : TServiceBase(
            hydraManager->CreateGuardedAutomatonInvoker(automatonInvoker),
            TServiceId(THiveServiceProxy::GetServiceName(), cellGuid),
            HiveLogger.GetCategory())
        , TCompositeAutomatonPart(
            hydraManager,
            automaton)
        , SelfCellGuid(cellGuid)
        , Config(config)
        , CellRegistry(cellRegistry)
        , RpcServer(rpcServer)
        , AutomatonInvoker(automatonInvoker)
    {
        Automaton->RegisterPart(this);

        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(Ping));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(Send));

        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraAcknowledgeMessages, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraReceiveMessages, Unretained(this), nullptr));

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

    void Start()
    {
        RpcServer->RegisterService(this);
    }

    void Stop()
    {
        RpcServer->UnregisterService(this);
    }


    const TCellGuid& GetSelfCellGuid() const
    {
        return SelfCellGuid;
    }


    TMailbox* CreateMailbox(const TCellGuid& cellGuid)
    {
        auto* mailbox = new TMailbox(cellGuid);
        MailboxMap.Insert(cellGuid, mailbox);
        
        if (IsLeader()) {
            SendPing(mailbox);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Mailbox created (SrcCellGuid: %s, DstCellGuid: %s)",
            ~ToString(SelfCellGuid),
            ~ToString(mailbox->GetCellGuid()));
        return mailbox;
    }
    
    TMailbox* GetOrCreateMailbox(const TCellGuid& cellGuid)
    {
        auto* mailbox = MailboxMap.Find(cellGuid);
        if (!mailbox) {
            mailbox = CreateMailbox(cellGuid);
        }
        return mailbox;
    }

    TMailbox* GetMailboxOrThrow(const TCellGuid& cellGuid)
    {
        auto* mailbox = FindMailbox(cellGuid);
        if (!mailbox) {
            THROW_ERROR_EXCEPTION("No such mailbox %s",
                ~ToString(cellGuid));
        }
        return mailbox;
    }

    void RemoveMailbox(const TCellGuid& cellGuid)
    {
        MailboxMap.Remove(cellGuid);
        LOG_INFO_UNLESS(IsRecovery(), "Mailbox removed (SrcCellGuid: %s, DstCellGuid: %s)",
            ~ToString(SelfCellGuid),
            ~ToString(cellGuid));
    }

    void PostMessage(TMailbox* mailbox, const TMessage& message)
    {
        int messageId =
            mailbox->GetFirstPendingMessageId() +
            static_cast<int>(mailbox->PendingMessages().size());
        mailbox->PendingMessages().push_back(message);
        LOG_DEBUG_UNLESS(IsRecovery(), "Message posted (SrcCellGuid: %s, DstCellGuid: %s, MessageId: %d)",
            ~ToString(SelfCellGuid),
            ~ToString(mailbox->GetCellGuid()),
            messageId);

        MaybeSendPendingMessages(mailbox);
    }

    void PostMessage(TMailbox* mailbox, const ::google::protobuf::MessageLite& message)
    {
        TMessage serializedMessage;
        serializedMessage.Type = message.GetTypeName();
        YCHECK(SerializeToProtoWithEnvelope(message, &serializedMessage.Data));
        PostMessage(mailbox, serializedMessage);
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Mailbox, TMailbox, TCellGuid);

private:
    typedef TImpl TThis;
    
    TCellGuid SelfCellGuid;
    THiveManagerConfigPtr Config;
    TCellDirectoryPtr CellRegistry;
    IRpcServerPtr RpcServer;
    IInvokerPtr AutomatonInvoker;

    IInvokerPtr EpochAutomatonInvoker;

    TEntityMap<TCellGuid, TMailbox> MailboxMap;
    

    // RPC handlers.

    DECLARE_RPC_SERVICE_METHOD(NProto, Ping)
    {
        auto srcCellGuid = FromProto<TCellGuid>(request->src_cell_guid());

        context->SetRequestInfo("SrcCellGuid: %s, DstCellGuid: %s",
            ~ToString(srcCellGuid),
            ~ToString(SelfCellGuid));

        auto* mailbox = FindMailbox(srcCellGuid);
        int lastReceivedMessageId = mailbox ? mailbox->GetLastReceivedMessageId() : -1;

        response->set_last_received_message_id(lastReceivedMessageId);

        context->SetResponseInfo("LastReceivedMessageId: %d",
            lastReceivedMessageId);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Send)
    {
        auto srcCellGuid = FromProto<TCellGuid>(request->src_cell_guid());
        int firstMessageId = request->first_message_id();

        context->SetRequestInfo("SrcCellGuid: %s, DstCellGuid: %s, FirstMessageId: %d, MessageCount: %" PRISZT,
            ~ToString(srcCellGuid),
            ~ToString(SelfCellGuid),
            firstMessageId,
            request->Attachments().size());
        
        CreateReceiveMessagesMutation(context)
            ->OnSuccess(CreateRpcSuccessHandler(context))
            ->OnError(CreateRpcErrorHandler(context))
            ->Commit();
    }


    // Hydra handlers.

    void HydraAcknowledgeMessages(const TReqAcknowledgeMessages& request)
    {
        auto cellGuid = FromProto<TCellGuid>(request.cell_guid());
        auto* mailbox = FindMailbox(cellGuid);
        if (!mailbox)
            return;

        int lastReceivedMessageId = request.last_received_message_id();
        int trimCount = lastReceivedMessageId - mailbox->GetFirstPendingMessageId() + 1;
        YCHECK(trimCount >= 0);
        if (trimCount == 0)
            return;

        auto& pendingMessages = mailbox->PendingMessages();
        std::move(
            pendingMessages.begin() + trimCount,
            pendingMessages.end(),
            pendingMessages.begin());
        pendingMessages.resize(pendingMessages.size() - trimCount);

        mailbox->SetFirstPendingMessageId(mailbox->GetFirstPendingMessageId() + trimCount);
        LOG_DEBUG_UNLESS(IsRecovery(), "Messages acknowledged (SrcCellGuid: %s, DstCellGuid: %s, FirstPendingMessageId: %d)",
            ~ToString(SelfCellGuid),
            ~ToString(mailbox->GetCellGuid()),
            mailbox->GetFirstPendingMessageId());

        if (IsLeader()) {
            YCHECK(mailbox->GetInFlightMessageCount() >= trimCount);
            mailbox->SetInFlightMessageCount(mailbox->GetInFlightMessageCount() - trimCount);
        }
    }

    void HydraReceiveMessages(TCtxSendPtr context, const TReqSend& request)
    {
        try {
            auto srcCellGuid = FromProto<TCellGuid>(request.src_cell_guid());
            auto* mailbox = GetOrCreateMailbox(srcCellGuid);

            HandleIncomingMessages(mailbox, request);

            if (context) {
                auto* response = &context->Response();
                int lastReceivedMessageId = mailbox->GetLastReceivedMessageId();
                response->set_last_received_message_id(lastReceivedMessageId);
                context->SetResponseInfo("LastReceivedMessageId: %d",
                    lastReceivedMessageId);
            }
        } catch (const std::exception& ex) {
            if (context) {
                context->Reply(ex);
            }
        }
    }


    IChannelPtr GetMailboxChannel(TMailbox* mailbox)
    {
        return CellRegistry->GetChannel(mailbox->GetCellGuid());
    }


    void SetMailboxConnected(TMailbox* mailbox)
    {
        if (mailbox->GetConnected())
            return;

        mailbox->SetConnected(true);
        LOG_INFO("Mailbox connected (SrcCellGuid: %s, DstCellGuid: %s)",
            ~ToString(SelfCellGuid),
            ~ToString(mailbox->GetCellGuid()));
    }

    void SetMailboxDisconnected(TMailbox* mailbox)
    {
        if (!mailbox->GetConnected())
            return;

        mailbox->SetConnected(false);
        mailbox->SetInFlightMessageCount(0);
        LOG_INFO("Mailbox disconnected (SrcCellGuid: %s, DstCellGuid: %s)",
            ~ToString(SelfCellGuid),
            ~ToString(mailbox->GetCellGuid()));
    }


    void SchedulePing(TMailbox* mailbox)
    {
        TDelayedExecutor::Submit(
            BIND(&TImpl::OnPingTick, MakeWeak(this), mailbox->GetCellGuid())
                .Via(EpochAutomatonInvoker),
            Config->PingPeriod);
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

        LOG_DEBUG("Sending ping (SrcCellGuid: %s, DstCellGuid: %s)",
            ~ToString(SelfCellGuid),
            ~ToString(mailbox->GetCellGuid()));

        THiveServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(Config->RpcTimeout);
        
        auto req = proxy.Ping();
        ToProto(req->mutable_src_cell_guid(), SelfCellGuid);
        req->Invoke().Subscribe(
            BIND(&TImpl::OnPingResponse, MakeStrong(this), mailbox->GetCellGuid())
                .Via(EpochAutomatonInvoker));
    }

    void OnPingResponse(const TCellGuid& cellGuid, THiveServiceProxy::TRspPingPtr rsp)
    {
        auto* mailbox = FindMailbox(cellGuid);
        if (!mailbox)
            return;

        if (!rsp->IsOK()) {
            LOG_DEBUG(*rsp, "Ping failed (SrcCellGuid: %s, DstCellGuid: %s)",
                ~ToString(SelfCellGuid),
                ~ToString(mailbox->GetCellGuid()));
            SchedulePing(mailbox);
            return;
        }

        int lastReceivedMessageId = rsp->last_received_message_id();
        LOG_DEBUG(*rsp, "Ping succeeded (SrcCellGuid: %s, DstCellGuid: %s, LastReceivedMessagId: %d)",
            ~ToString(SelfCellGuid),
            ~ToString(mailbox->GetCellGuid()),
            lastReceivedMessageId);

        SetMailboxConnected(mailbox);
        HandleAcknowledgedMessages(mailbox, lastReceivedMessageId);
        MaybeSendPendingMessages(mailbox);
        SchedulePing(mailbox);
    }


    void MaybeSendPendingMessages(TMailbox* mailbox)
    {
        if (!IsLeader())
            return;

        if (!mailbox->GetConnected())
            return;

        auto channel = GetMailboxChannel(mailbox);
        if (!channel)
            return;

        if (mailbox->PendingMessages().size() <= mailbox->GetInFlightMessageCount())
            return;

        THiveServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(Config->RpcTimeout);

        auto req = proxy.Send();
        ToProto(req->mutable_src_cell_guid(), SelfCellGuid);
        int firstMessageId = mailbox->GetFirstPendingMessageId() + mailbox->GetInFlightMessageCount();
        int messageCount = mailbox->PendingMessages().size() - mailbox->GetInFlightMessageCount();
        req->set_first_message_id(firstMessageId);
        for (auto it = mailbox->PendingMessages().begin() + mailbox->GetInFlightMessageCount();
             it != mailbox->PendingMessages().end();
             ++it)
        {
            const auto& message = *it;
            req->add_messages(SerializeMessage(message));
        }

        mailbox->SetInFlightMessageCount(mailbox->GetInFlightMessageCount() + messageCount);

        LOG_DEBUG("Pending messages sent (SrcCellGuid: %s, DstCellGuid: %s, MessageIds: %d-%d)",
            ~ToString(SelfCellGuid),
            ~ToString(mailbox->GetCellGuid()),
            firstMessageId,
            firstMessageId + messageCount - 1);

        req->Invoke().Subscribe(
            BIND(&TImpl::OnSendResponse, MakeStrong(this), mailbox->GetCellGuid())
                .Via(EpochAutomatonInvoker));
    }

    void OnSendResponse(const TCellGuid& cellGuid, THiveServiceProxy::TRspSendPtr rsp)
    {
        auto* mailbox = FindMailbox(cellGuid);
        if (!mailbox)
            return;

        if (!rsp->IsOK()) {
            LOG_DEBUG(*rsp, "Send failed (SrcCellGuid: %s, DstCellGuid: %s)",
                ~ToString(SelfCellGuid),
                ~ToString(mailbox->GetCellGuid()));
            SetMailboxDisconnected(mailbox);
            return;
        }

        int lastReceivedMessageId = rsp->last_received_message_id();
        LOG_DEBUG(*rsp, "Send succeeded (SrcCellGuid: %s, DstCellGuid: %s, LastReceivedMessageId: %d)",
            ~ToString(SelfCellGuid),
            ~ToString(mailbox->GetCellGuid()),
            lastReceivedMessageId);

        HandleAcknowledgedMessages(mailbox, lastReceivedMessageId);
    }


    TMutationPtr CreateAcknowledgeMessagesMutation(const TReqAcknowledgeMessages& req)
    {
        return CreateMutation(
            HydraManager,
            AutomatonInvoker,
            req,
            this,
            &TImpl::HydraAcknowledgeMessages);
    }

    TMutationPtr CreateReceiveMessagesMutation(TCtxSendPtr context)
    {
        return CreateMutation(HydraManager, AutomatonInvoker)
            ->SetRequestData(context->GetRequestBody())
            ->SetType(context->Request().GetTypeName())
            ->SetAction(BIND(&TImpl::HydraReceiveMessages, MakeStrong(this), context, ConstRef(context->Request())));
    }


    void HandleAcknowledgedMessages(TMailbox* mailbox, int lastReceivedMessageId)
    {
        if (lastReceivedMessageId < mailbox->GetFirstPendingMessageId())
            return;

        TReqAcknowledgeMessages req;
        ToProto(req.mutable_cell_guid(), mailbox->GetCellGuid());
        req.set_last_received_message_id(lastReceivedMessageId);
        CreateAcknowledgeMessagesMutation(req)
            ->Commit();
    }

    void HandleIncomingMessages(TMailbox* mailbox, const TReqSend& req)
    {
        int firstMessageId = req.first_message_id();
        int skipCount = mailbox->GetLastReceivedMessageId() + 1 - firstMessageId;
        YCHECK(skipCount >= 0);
        
        for (int index = skipCount; index < req.messages_size(); ++index) {
            int messageId = firstMessageId + index;
            mailbox->SetLastReceivedMessageId(messageId);
            LOG_DEBUG_UNLESS(IsRecovery(), "Message received (SrcCellGuid: %s, DstCellGuid: %s, MessageId: %d)",
                ~ToString(mailbox->GetCellGuid()),
                ~ToString(SelfCellGuid),
                messageId);

            auto request = DeserializeMessage(req.messages(index));
            TMutationContext context(
                HydraManager->GetMutationContext(),
                request);

            auto* automaton = static_cast<IAutomaton*>(Automaton);
            automaton->ApplyMutation(&context);
        }
    }


    static Stroka SerializeMessage(const TMessage& message)
    {
        Stroka serializedMessage;
        TStringOutput output(serializedMessage);
        NYT::TStreamSaveContext context(&output);
        Save(context, message.Type);
        Save(context, message.Data);
        return serializedMessage;
    }

    static TMutationRequest DeserializeMessage(const Stroka& serializedMessage)
    {
        TMutationRequest request;
        TStringInput input(serializedMessage);
        NYT::TStreamLoadContext context(&input);
        Load(context, request.Type);
        Load(context, request.Data);
        return request;
    }


    virtual void OnLeaderActive() override
    {
        EpochAutomatonInvoker = HydraManager
            ->GetEpochContext()
            ->CancelableContext
            ->CreateInvoker(AutomatonInvoker);

        for (const auto& pair : MailboxMap) {
            auto* mailbox = pair.second;
            mailbox->SetInFlightMessageCount(0);
            mailbox->SetConnected(false);
            SendPing(mailbox);
        }
    }

    virtual void OnStopLeading() override
    {
        for (const auto& pair : MailboxMap) {
            auto* mailbox = pair.second;
            mailbox->SetInFlightMessageCount(0);
        }

        EpochAutomatonInvoker.Reset();
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
        MailboxMap.Clear();
    }

    void SaveKeys(TSaveContext& context) const
    {
        MailboxMap.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context) const
    {
        MailboxMap.SaveValues(context);
    }

    void LoadKeys(TLoadContext& context)
    {
        MailboxMap.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        MailboxMap.LoadValues(context);
    }

};

DEFINE_ENTITY_MAP_ACCESSORS(THiveManager::TImpl, Mailbox, TMailbox, TCellGuid, MailboxMap)

////////////////////////////////////////////////////////////////////////////////

THiveManager::THiveManager(
    const TCellGuid& selfCellGuid,
    THiveManagerConfigPtr config,
    TCellDirectoryPtr cellRegistry,
    IInvokerPtr automatonInvoker,
    IRpcServerPtr rpcServer,
    IHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton)
    : Impl(New<TImpl>(
        selfCellGuid,
        config,
        cellRegistry,
        automatonInvoker,
        rpcServer,
        hydraManager,
        automaton))
{ }

void THiveManager::Start()
{
    Impl->Start();
}

void THiveManager::Stop()
{
    Impl->Stop();
}

const TCellGuid& THiveManager::GetSelfCellGuid() const
{
    return Impl->GetSelfCellGuid();
}

TMailbox* THiveManager::CreateMailbox(const TCellGuid& cellGuid)
{
    return Impl->CreateMailbox(cellGuid);
}

TMailbox* THiveManager::GetOrCreateMailbox(const TCellGuid& cellGuid)
{
    return Impl->GetOrCreateMailbox(cellGuid);
}

TMailbox* THiveManager::GetMailboxOrThrow(const TCellGuid& cellGuid)
{
    return Impl->GetMailboxOrThrow(cellGuid);
}

void THiveManager::RemoveMailbox(const TCellGuid& cellGuid)
{
    Impl->RemoveMailbox(cellGuid);
}

void THiveManager::PostMessage(TMailbox* mailbox, const TMessage& message)
{
    Impl->PostMessage(mailbox, message);
}

void THiveManager::PostMessage(TMailbox* mailbox, const ::google::protobuf::MessageLite& message)
{
    Impl->PostMessage(mailbox, message);
}

DELEGATE_ENTITY_MAP_ACCESSORS(THiveManager, Mailbox, TMailbox, TCellGuid, *Impl)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
