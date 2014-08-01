#include "stdafx.h"
#include "object_service.h"
#include "private.h"
#include "object_manager.h"
#include "config.h"

#include <core/ytree/ypath_detail.h>

#include <core/rpc/message.h>
#include <core/rpc/service_detail.h>
#include <core/rpc/helpers.h>

#include <core/actions/invoker_util.h>

#include <ytlib/security_client/public.h>

#include <ytlib/hydra/rpc_helpers.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <server/transaction_server/transaction.h>
#include <server/transaction_server/transaction_manager.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/hydra_facade.h>
#include <server/cell_master/hydra_service.h>

#include <server/security_server/security_manager.h>
#include <server/security_server/user.h>

#include <server/cypress_server/cypress_manager.h>

#include <atomic>

namespace NYT {
namespace NObjectServer {

using namespace NHydra;
using namespace NRpc;
using namespace NRpc::NProto;
using namespace NBus;
using namespace NYTree;
using namespace NYTree::NProto;
using namespace NCypressServer;
using namespace NTransactionServer;
using namespace NSecurityClient;
using namespace NSecurityServer;
using namespace NObjectServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TObjectService
    : public NCellMaster::THydraServiceBase
{
public:
    TObjectService(
        TObjectManagerConfigPtr config,
        TBootstrap* bootstrap)
        : THydraServiceBase(
            bootstrap,
            NObjectClient::TObjectServiceProxy::GetServiceName(),
            ObjectServerLogger)
        , Config(config)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GCCollect));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(BuildSnapshot));
    }

private:
    class TExecuteSession;

    TObjectManagerConfigPtr Config;

    DECLARE_RPC_SERVICE_METHOD(NObjectClient::NProto, Execute);
    DECLARE_RPC_SERVICE_METHOD(NObjectClient::NProto, GCCollect);
    DECLARE_RPC_SERVICE_METHOD(NObjectClient::NProto, BuildSnapshot);

};

IServicePtr CreateObjectService(
    TObjectManagerConfigPtr config,
    TBootstrap* bootstrap)
{
    return New<TObjectService>(
        config,
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

class TObjectService::TExecuteSession
    : public TIntrinsicRefCounted
{
public:
    TExecuteSession(
        TBootstrap* boostrap,
        TObjectManagerConfigPtr config,
        TCtxExecutePtr context)
        : Bootstrap(boostrap)
        , Config(std::move(config))
        , Context(std::move(context))
        , LastMutationCommitted(VoidFuture)
        , Replied(false)
        , ResponseCount(0)
        , CurrentRequestIndex(0)
        , CurrentRequestPartIndex(0)
        , Logger(ObjectServerLogger)
    { }

    void Run()
    {
        int requestCount = Context->Request().part_counts_size();
        UserName = FindAuthenticatedUser(Context);

        Context->SetRequestInfo("RequestCount: %v", requestCount);

        auto* user = GetAuthenticatedUser();

        auto securityManager = Bootstrap->GetSecurityManager();
        securityManager->ValidateUserAccess(user, requestCount);

        ResponseMessages.resize(requestCount);
        RequestHeaders.resize(requestCount);

        if (requestCount == 0) {
            Reply();
        } else {
            Continue();
        }
    }

private:
    TBootstrap* Bootstrap;
    TObjectManagerConfigPtr Config;
    TCtxExecutePtr Context;

    TFuture<void> LastMutationCommitted;
    std::atomic<bool> Replied;
    std::atomic<int> ResponseCount;
    std::vector<TSharedRefArray> ResponseMessages;
    std::vector<TRequestHeader> RequestHeaders;
    int CurrentRequestIndex;
    int CurrentRequestPartIndex;
    TNullable<Stroka> UserName;

    const NLog::TLogger& Logger;

    void Continue()
    {
        try {
            auto objectManager = Bootstrap->GetObjectManager();
            auto rootService = objectManager->GetRootService();

            auto hydraFacade = Bootstrap->GetHydraFacade();

            auto startTime = TInstant::Now();
            auto& request = Context->Request();
            const auto& attachments = request.Attachments();
            
            ValidatePrerequisites();

            auto securityManager = Bootstrap->GetSecurityManager();           
            auto* user = GetAuthenticatedUser();
            TAuthenticatedUserGuard userGuard(securityManager, user);

            while (CurrentRequestIndex < request.part_counts_size()) {
                // Don't allow the thread to be blocked for too long by a single batch.
                if (TInstant::Now() > startTime + Config->YieldTimeout) {
                    hydraFacade->GetEpochAutomatonInvoker()->Invoke(
                        BIND(&TExecuteSession::Continue, MakeStrong(this)));
                    return;
                }

                int partCount = request.part_counts(CurrentRequestIndex);
                if (partCount == 0) {
                    // Skip empty requests.
                    OnResponse(
                        CurrentRequestIndex,
                        false,
                        NTracing::TTraceContext(),
                        nullptr,
                        TSharedRefArray());
                    NextRequest();
                    continue;
                }

                std::vector<TSharedRef> requestParts(
                    attachments.begin() + CurrentRequestPartIndex,
                    attachments.begin() + CurrentRequestPartIndex + partCount);

                auto requestMessage = TSharedRefArray(std::move(requestParts));

                auto& requestHeader = RequestHeaders[CurrentRequestIndex];
                if (!ParseRequestHeader(requestMessage, &requestHeader)) {
                    THROW_ERROR_EXCEPTION(
                        NRpc::EErrorCode::ProtocolError,
                        "Error parsing request header");
                }

                const auto& requestHeaderExt = requestHeader.GetExtension(TYPathHeaderExt::ypath_header_ext);
                const auto& path = requestHeaderExt.path();

                auto mutationId = GetMutationId(requestHeader);
                bool mutating = requestHeaderExt.mutating();

                // Forbid to reorder read requests before write ones.
                if (!mutating && !LastMutationCommitted.IsSet()) {
                    LastMutationCommitted.Subscribe(
                        BIND(&TExecuteSession::Continue, MakeStrong(this))
                            .Via(hydraFacade->GetEpochAutomatonInvoker()));
                    return;
                }

                LOG_DEBUG("Execute[%v] <- %v:%v %v (RequestId: %v, Mutating: %v, MutationId: %v)",
                    CurrentRequestIndex,
                    ~requestHeader.service(),
                    ~requestHeader.method(),
                    ~path,
                    Context->GetRequestId(),
                    mutating,
                    mutationId);

                NTracing::TTraceContextGuard traceContextGuard(NTracing::CreateChildTraceContext());
                NTracing::TraceEvent(
                    ~requestHeader.service(),
                    ~requestHeader.method(),
                    NTracing::ServerReceiveAnnotation);

                auto asyncResponseMessage = ExecuteVerb(
                    rootService,
                    std::move(requestMessage));

                // Optimize for the (typical) case of synchronous response.
                if (asyncResponseMessage.IsSet() &&
                    TInstant::Now() < startTime + Config->YieldTimeout)
                {
                    OnResponse(
                        CurrentRequestIndex,
                        mutating,
                        traceContextGuard.GetContext(),
                        &requestHeader,
                        asyncResponseMessage.Get());
                } else {
                    LastMutationCommitted = asyncResponseMessage.Apply(BIND(
                        &TExecuteSession::OnResponse,
                        MakeStrong(this),
                        CurrentRequestIndex,
                        mutating,
                        traceContextGuard.GetContext(),
                        &requestHeader));
                }

                NextRequest();
            }
        } catch (const std::exception& ex) {
            Reply(ex);
        }
    }

    void OnResponse(
        int requestIndex,
        bool mutating,
        const NTracing::TTraceContext& traceContext,
        const TRequestHeader* requestHeader,
        TSharedRefArray responseMessage)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (responseMessage) {
            NTracing::TraceEvent(
                traceContext,
                requestHeader->service(),
                requestHeader->method(),
                NTracing::ServerSendAnnotation);

            TResponseHeader responseHeader;
            YCHECK(ParseResponseHeader(responseMessage, &responseHeader));

            auto error = FromProto<TError>(responseHeader.error());

            LOG_DEBUG("Execute[%v] -> Error: %v (RequestId: %v)",
                requestIndex,
                error,
                Context->GetRequestId());

            if (mutating && error.GetCode() == NRpc::EErrorCode::Unavailable) {
                // Commit failed -- stop further handling.
                Context->Reply(error);
                return;
            }
        }

        ResponseMessages[requestIndex] = std::move(responseMessage);

        if (++ResponseCount == ResponseMessages.size()) {
            Reply();
        }
    }

    void NextRequest()
    {
        const auto& request = Context->Request();
        CurrentRequestPartIndex += request.part_counts(CurrentRequestIndex);
        CurrentRequestIndex += 1;
    }

    void Reply(const TError& error = TError())
    {
        bool expected = false;
        if (!Replied.compare_exchange_strong(expected, true))
            return;

        if (error.IsOK()) {
            auto& response = Context->Response();
            for (const auto& responseMessage : ResponseMessages) {
                if (responseMessage) {
                    response.add_part_counts(responseMessage.Size());
                    response.Attachments().insert(
                        response.Attachments().end(),
                        responseMessage.Begin(),
                        responseMessage.End());
                } else {
                    response.add_part_counts(0);
                }
            }
        }
     
        Context->Reply(error);
    }

    void ValidatePrerequisites()
    {       
        auto& request = Context->Request();

        for (const auto& prerequisite : request.prerequisite_transactions()) {
            auto transactionId = FromProto<TTransactionId>(prerequisite.transaction_id());
            GetPrerequisiteTransaction(transactionId);
        }

        auto cypressManager = Bootstrap->GetCypressManager();
        for (const auto& prerequisite : request.prerequisite_revisions()) {
            auto transactionId = FromProto<TTransactionId>(prerequisite.transaction_id());
            const auto& path = prerequisite.path();
            i64 revision = prerequisite.revision();

            auto* transaction =
                transactionId == NullTransactionId
                ? nullptr
                : GetPrerequisiteTransaction(transactionId);

            auto resolver = cypressManager->CreateResolver(transaction);
            INodePtr nodeProxy;
            try {
                nodeProxy = resolver->ResolvePath(path);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION(
                    NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                    "Prerequisite check failed: failed to resolve path %v",
                    ~path)
                    << ex;
            }

            auto* cypressNodeProxy = dynamic_cast<ICypressNodeProxy*>(nodeProxy.Get());
            YCHECK(cypressNodeProxy);

            auto* node = cypressNodeProxy->GetTrunkNode();
            if (node->GetRevision() != revision) {
                THROW_ERROR_EXCEPTION(
                    NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                    "Prerequisite check failed: node %v revision mismatch: expected %v" ", found %v",
                    ~path,
                    revision,
                    node->GetRevision());
            }
        }
    }

    TTransaction* GetPrerequisiteTransaction(const TTransactionId& transactionId)
    {
        auto transactionManager = Bootstrap->GetTransactionManager();
        auto* transaction = transactionManager->FindTransaction(transactionId);
        if (!IsObjectAlive(transaction)) {
            THROW_ERROR_EXCEPTION(
                NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                "Prerequisite check failed: transaction %v is missing",
                transactionId);
        }
        if (transaction->GetState() != ETransactionState::Active) {
            THROW_ERROR_EXCEPTION(
                NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                "Prerequisite check failed: transaction %v is not active",
                transactionId);
        }
        return transaction;
    }

    TUser* GetAuthenticatedUser()
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        return UserName
            ? securityManager->GetUserByNameOrThrow(*UserName)
            : securityManager->GetRootUser();
    }

};

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TObjectService, Execute)
{
    UNUSED(request);
    UNUSED(response);

    ValidateActiveLeader();

    auto session = New<TExecuteSession>(
        Bootstrap,
        Config,
        std::move(context));
    session->Run();
}

DEFINE_RPC_SERVICE_METHOD(TObjectService, GCCollect)
{
    UNUSED(request);
    UNUSED(response);

    context->SetRequestInfo();

    auto objectManager = Bootstrap->GetObjectManager();
    objectManager->GCCollect().Subscribe(BIND([=] () {
        context->Reply();
    }));
}

DEFINE_RPC_SERVICE_METHOD(TObjectService, BuildSnapshot)
{
    bool setReadOnly = request->set_read_only();

    context->SetRequestInfo("SetReadOnly: %v",
        setReadOnly);

    auto hydraManager = Bootstrap->GetHydraFacade()->GetHydraManager();

    if (setReadOnly) {
        hydraManager->SetReadOnly(true);
    }

    hydraManager->BuildSnapshotDistributed().Subscribe(BIND([=] (TErrorOr<int> errorOrSnapshotId) {
        if (!errorOrSnapshotId.IsOK()) {
            context->Reply(TError(errorOrSnapshotId));
            return;
        }

        response->set_snapshot_id(errorOrSnapshotId.Value());
        context->Reply();
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
