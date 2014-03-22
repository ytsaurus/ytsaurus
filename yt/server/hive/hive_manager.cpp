#include "stdafx.h"
#include "hive_manager.h"
#include "config.h"
#include "hive_service_proxy.h"
#include "mailbox.h"
#include "private.h"

#include <core/rpc/service_detail.h>
#include <core/rpc/server.h>

#include <core/concurrency/delayed_executor.h>

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
    : public THydraServiceBase
    , public TCompositeAutomatonPart
{
public:
    TImpl(
        const TCellGuid& cellGuid,
        THiveManagerConfigPtr config,
        TCellDirectoryPtr cellDirectory,
        IInvokerPtr automatonInvoker,
        NRpc::IServerPtr rpcServer,
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
        , SelfCellGuid(cellGuid)
        , Config(config)
        , CellDirectory(cellDirectory)
        , RpcServer(rpcServer)
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
        // A typical mistake is to try sending a Hive message outside of a mutation.
        YCHECK(HydraManager->IsMutating());

        int messageId =
            mailbox->GetFirstOutcomingMessageId() +
            static_cast<int>(mailbox->OutcomingMessages().size());
        auto serializedMessage = SerializeMessage(message);
        mailbox->OutcomingMessages().push_back(serializedMessage);

        LOG_DEBUG_UNLESS(IsRecovery(), "Outcoming message added (SrcCellGuid: %s, DstCellGuid: %s, MessageId: %d)",
            ~ToString(SelfCellGuid),
            ~ToString(mailbox->GetCellGuid()),
            messageId);

        MaybeSendOutcomingMessages(mailbox);
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
    TCellDirectoryPtr CellDirectory;
    IServerPtr RpcServer;

    TEntityMap<TCellGuid, TMailbox> MailboxMap;
    

    // RPC handlers.

    DECLARE_RPC_SERVICE_METHOD(NProto, Ping)
    {
        auto srcCellGuid = FromProto<TCellGuid>(request->src_cell_guid());

        context->SetRequestInfo("SrcCellGuid: %s, DstCellGuid: %s",
            ~ToString(srcCellGuid),
            ~ToString(SelfCellGuid));

        auto* mailbox = FindMailbox(srcCellGuid);
        int lastIncomingMessageId = mailbox ? mailbox->GetLastIncomingMessageId() : -1;

        response->set_last_incoming_message_id(lastIncomingMessageId);

        context->SetResponseInfo("LastIncomingMessageId: %d",
            lastIncomingMessageId);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, PostMessages)
    {
        auto srcCellGuid = FromProto<TCellGuid>(request->src_cell_guid());
        int firstMessageId = request->first_message_id();

        context->SetRequestInfo("SrcCellGuid: %s, DstCellGuid: %s, MessageIds: %d-%d",
            ~ToString(srcCellGuid),
            ~ToString(SelfCellGuid),
            firstMessageId,
            firstMessageId + request->messages_size() - 1);
        
        CreateReceiveMessagesMutation(context)
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
        LOG_DEBUG_UNLESS(IsRecovery(), "Messages acknowledged (SrcCellGuid: %s, DstCellGuid: %s, FirstOutcomingMessageId: %d)",
            ~ToString(SelfCellGuid),
            ~ToString(mailbox->GetCellGuid()),
            mailbox->GetFirstOutcomingMessageId());

        if (IsLeader()) {
            YCHECK(mailbox->GetInFlightMessageCount() >= trimCount);
            mailbox->SetInFlightMessageCount(mailbox->GetInFlightMessageCount() - trimCount);
        }
    }

    void HydraPostMessages(TCtxPostMessagesPtr context, const TReqPostMessages& request)
    {
        try {
            auto srcCellGuid = FromProto<TCellGuid>(request.src_cell_guid());
            auto* mailbox = GetOrCreateMailbox(srcCellGuid);

            HandleIncomingMessages(mailbox, request);

            if (context) {
                auto* response = &context->Response();
                int lastIncomingMessageId = mailbox->GetLastIncomingMessageId();
                response->set_last_incoming_message_id(lastIncomingMessageId);
                context->SetResponseInfo("LastIncomingMessageId: %d",
                    lastIncomingMessageId);
            }
        } catch (const std::exception& ex) {
            if (context) {
                context->Reply(ex);
            }
        }
    }


    IChannelPtr GetMailboxChannel(TMailbox* mailbox)
    {
        return CellDirectory->FindChannel(mailbox->GetCellGuid());
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
                .Via(EpochAutomatonInvoker_),
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
                .Via(EpochAutomatonInvoker_));
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

        int lastIncomingMessageId = rsp->last_incoming_message_id();
        LOG_DEBUG("Ping succeeded (SrcCellGuid: %s, DstCellGuid: %s, LastReceivedMessagId: %d)",
            ~ToString(SelfCellGuid),
            ~ToString(mailbox->GetCellGuid()),
            lastIncomingMessageId);

        SetMailboxConnected(mailbox);
        HandleAcknowledgedMessages(mailbox, lastIncomingMessageId);
        MaybeSendOutcomingMessages(mailbox);
        SchedulePing(mailbox);
    }


    void MaybeSendOutcomingMessages(TMailbox* mailbox)
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
        proxy.SetDefaultTimeout(Config->RpcTimeout);

        auto req = proxy.PostMessages();
        ToProto(req->mutable_src_cell_guid(), SelfCellGuid);
        int firstMessageId = mailbox->GetFirstOutcomingMessageId() + mailbox->GetInFlightMessageCount();
        int messageCount = mailbox->OutcomingMessages().size() - mailbox->GetInFlightMessageCount();
        req->set_first_message_id(firstMessageId);
        for (auto it = mailbox->OutcomingMessages().begin() + mailbox->GetInFlightMessageCount();
             it != mailbox->OutcomingMessages().end();
             ++it)
        {
            req->add_messages(*it);
        }

        mailbox->SetInFlightMessageCount(mailbox->GetInFlightMessageCount() + messageCount);

        LOG_DEBUG("Posting outcoming messages (SrcCellGuid: %s, DstCellGuid: %s, MessageIds: %d-%d)",
            ~ToString(SelfCellGuid),
            ~ToString(mailbox->GetCellGuid()),
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
            LOG_DEBUG(*rsp, "Failed to post outcoming messages (SrcCellGuid: %s, DstCellGuid: %s)",
                ~ToString(SelfCellGuid),
                ~ToString(mailbox->GetCellGuid()));
            SetMailboxDisconnected(mailbox);
            return;
        }

        int lastIncomingMessageId = rsp->last_incoming_message_id();
        LOG_DEBUG("Outcoming messages posted successfully (SrcCellGuid: %s, DstCellGuid: %s, LastIncomingMessageId: %d)",
            ~ToString(SelfCellGuid),
            ~ToString(mailbox->GetCellGuid()),
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

    TMutationPtr CreateReceiveMessagesMutation(TCtxPostMessagesPtr context)
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

    void HandleIncomingMessage(TMailbox* mailbox, int messageId, const Stroka& serializedMessage)
    {
        if (messageId <= mailbox->GetLastIncomingMessageId()) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Dropping an obsolete incoming message (SrcCellGuid: %s, DstCellGuid: %s, MessageId: %d)",
                ~ToString(mailbox->GetCellGuid()),
                ~ToString(SelfCellGuid),
                messageId);
        } else {
            auto& incomingMessages = mailbox->IncomingMessages();
            YCHECK(incomingMessages.insert(std::make_pair(messageId, serializedMessage)).second);

            bool consumed = false;
            while (!incomingMessages.empty()) {
                const auto& frontPair = *incomingMessages.begin();
                int frontMessageId = frontPair.first;
                const auto& serializedFrontMessage = frontPair.second;
                if (frontMessageId != mailbox->GetLastIncomingMessageId() + 1)
                    break;

                LOG_DEBUG_UNLESS(IsRecovery(), "Consuming incoming message (SrcCellGuid: %s, DstCellGuid: %s, MessageId: %d)",
                    ~ToString(mailbox->GetCellGuid()),
                    ~ToString(SelfCellGuid),
                    frontMessageId);

                auto request = DeserializeMessage(serializedFrontMessage);
                YCHECK(incomingMessages.erase(frontMessageId) == 1);
                mailbox->SetLastIncomingMessageId(frontMessageId);
                consumed = true;

                TMutationContext context(HydraManager->GetMutationContext(), request);
                static_cast<IAutomaton*>(Automaton)->ApplyMutation(&context);
            }

            if (!consumed) {
                LOG_DEBUG_UNLESS(IsRecovery(), "Keeping an out-of-order incoming message (SrcCellGuid: %s, DstCellGuid: %s, MessageId: %d)",
                    ~ToString(mailbox->GetCellGuid()),
                    ~ToString(SelfCellGuid),
                    messageId);
            }
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
    TCellDirectoryPtr cellDirectory,
    IInvokerPtr automatonInvoker,
    IServerPtr rpcServer,
    IHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton)
    : Impl_(New<TImpl>(
        selfCellGuid,
        config,
        cellDirectory,
        automatonInvoker,
        rpcServer,
        hydraManager,
        automaton))
{ }

THiveManager::~THiveManager()
{ }

void THiveManager::Start()
{
    Impl_->Start();
}

void THiveManager::Stop()
{
    Impl_->Stop();
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

void THiveManager::PostMessage(TMailbox* mailbox, const TMessage& message)
{
    Impl_->PostMessage(mailbox, message);
}

void THiveManager::PostMessage(TMailbox* mailbox, const ::google::protobuf::MessageLite& message)
{
    Impl_->PostMessage(mailbox, message);
}

DELEGATE_ENTITY_MAP_ACCESSORS(THiveManager, Mailbox, TMailbox, TCellGuid, *Impl_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
