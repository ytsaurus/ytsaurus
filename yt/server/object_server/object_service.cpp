#include "stdafx.h"
#include "object_service.h"
#include "private.h"
#include "object_manager.h"
#include "config.h"

#include <core/ytree/ypath_detail.h>

#include <core/rpc/message.h>
#include <core/rpc/service_detail.h>
#include <core/rpc/helpers.h>

#include <core/concurrency/parallel_awaiter.h>

#include <core/actions/invoker_util.h>

#include <ytlib/security_client/public.h>

#include <ytlib/meta_state/rpc_helpers.h>

#include <server/transaction_server/transaction.h>
#include <server/transaction_server/transaction_manager.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>

#include <server/security_server/security_manager.h>
#include <server/security_server/user.h>

#include <server/cypress_server/cypress_manager.h>

namespace NYT {
namespace NObjectServer {

using namespace NMetaState;
using namespace NRpc;
using namespace NBus;
using namespace NYTree;
using namespace NCypressServer;
using namespace NTransactionServer;
using namespace NSecurityClient;
using namespace NSecurityServer;
using namespace NCellMaster;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = ObjectServerLogger;

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
        , Awaiter(New<TParallelAwaiter>(GetSyncInvoker()))
        , ReplyLock(0)
        , CurrentRequestIndex(0)
        , CurrentRequestPartIndex(0)
    { }

    void Run()
    {
        int requestCount = Context->Request().part_counts_size();
        UserName = FindAuthenticatedUser(Context);

        Context->SetRequestInfo("RequestCount: %d", requestCount);

        // Disable rate limits for ping commands as they are vital.
        bool enforceRateLimit = true;
        if (requestCount == 1) {
            TSharedRefArray requestMessage(Context->Request().Attachments());
            NRpc::NProto::TRequestHeader requestHeader;
            if (ParseRequestHeader(requestMessage, &requestHeader)) {
                if (requestHeader.verb() == "Ping") {
                    enforceRateLimit = false;
                }
            }
        }

        auto* user = GetAuthenticatedUser();

        auto securityManager = Bootstrap->GetSecurityManager();
        securityManager->ValidateUserAccess(user, requestCount, enforceRateLimit);

        ResponseMessages.resize(requestCount);
        Continue();
    }

private:
    TBootstrap* Bootstrap;
    TObjectManagerConfigPtr Config;
    TCtxExecutePtr Context;

    TParallelAwaiterPtr Awaiter;
    std::vector<TSharedRefArray> ResponseMessages;
    TAtomic ReplyLock;
    int CurrentRequestIndex;
    int CurrentRequestPartIndex;
    TNullable<Stroka> UserName;

    void Continue()
    {
        try {
            auto startTime = TInstant::Now();
            auto& request = Context->Request();
            const auto& attachments = request.Attachments();
            
            auto objectManager = Bootstrap->GetObjectManager();
            auto rootService = objectManager->GetRootService();

            auto metaStateManager = Bootstrap->GetMetaStateFacade()->GetManager();

            auto awaiter = Awaiter;
            if (!awaiter)
                return;

            if (!CheckPrerequisites())
                return;

            auto* user = GetAuthenticatedUser();
            TAuthenticatedUserGuard userGuard(Bootstrap->GetSecurityManager(), user);

            // Execute another portion of requests.
            while (CurrentRequestIndex < request.part_counts_size()) {
                int partCount = request.part_counts(CurrentRequestIndex);
                if (partCount == 0) {
                    // Skip empty requests.
                    ++CurrentRequestIndex;
                    continue;
                }

                std::vector<TSharedRef> requestParts(
                    attachments.begin() + CurrentRequestPartIndex,
                    attachments.begin() + CurrentRequestPartIndex + partCount);
                auto requestMessage = TSharedRefArray(std::move(requestParts));

                NRpc::NProto::TRequestHeader requestHeader;
                if (!ParseRequestHeader(requestMessage, &requestHeader)) {
                    Reply(TError(
                        NRpc::EErrorCode::ProtocolError,
                        "Error parsing request header"));
                    return;
                }

                const auto& path = requestHeader.path();
                const auto& verb = requestHeader.verb();
                auto mutationId = GetMutationId(requestHeader);

                LOG_DEBUG("Execute[%d] <- %s %s (RequestId: %s, MutationId: %s)",
                    CurrentRequestIndex,
                    ~verb,
                    ~path,
                    ~ToString(Context->GetRequestId()),
                    ~ToString(mutationId));

                if (AtomicGet(ReplyLock) != 0)
                    return;

                bool foundKeptResponse = false;
                if (mutationId != NullMutationId) {
                    auto keptResponse = metaStateManager->FindKeptResponse(mutationId);
                    if (keptResponse) {
                        auto responseMessage = TSharedRefArray::Unpack(keptResponse->Data);
                        OnResponse(CurrentRequestIndex, std::move(responseMessage));
                        foundKeptResponse = true;
                    }
                }

                if (!foundKeptResponse) {
                    awaiter->Await(
                        ExecuteVerb(rootService, requestMessage),
                        BIND(&TExecuteSession::OnResponse, MakeStrong(this), CurrentRequestIndex));
                }

                ++CurrentRequestIndex;
                CurrentRequestPartIndex += partCount;

                if (TInstant::Now() > startTime + Config->YieldTimeout) {
                    YieldAndContinue();
                    return;
                }
            }

            awaiter->Complete(BIND(&TExecuteSession::OnComplete, MakeStrong(this)));
        } catch (const std::exception& ex) {
            Reply(ex);
        }
    }

    void YieldAndContinue()
    {
        LOG_DEBUG("Yielding state thread (RequestId: %s)",
            ~ToString(Context->GetRequestId()));

        auto invoker = Bootstrap->GetMetaStateFacade()->GetGuardedInvoker();
        if (!invoker->Invoke(BIND(&TExecuteSession::Continue, MakeStrong(this)))) {
            Reply(TError(
                NRpc::EErrorCode::Unavailable,
                "Yield error, only %d of out %d requests were served",
                CurrentRequestIndex,
                Context->Request().part_counts_size()));
        }
    }

    void OnResponse(int requestIndex, TSharedRefArray responseMessage)
    {
        NRpc::NProto::TResponseHeader responseHeader;
        YCHECK(ParseResponseHeader(responseMessage, &responseHeader));

        auto error = FromProto(responseHeader.error());

        LOG_DEBUG("Execute[%d] -> Error: %s (RequestId: %s)",
            requestIndex,
            ~ToString(error),
            ~ToString(Context->GetRequestId()));

        if (error.GetCode() == NRpc::EErrorCode::Unavailable) {
            // Commit failed -- stop further handling.
            Reply(error);
        } else {
            // No sync is needed, requestIndexes are distinct.
            ResponseMessages[requestIndex] = responseMessage;
        }
    }

    void OnComplete()
    {
        // No sync is needed: OnComplete is called after all OnResponses.
        auto& response = Context->Response();

        FOREACH (const auto& responseMessage, ResponseMessages) {
            if (!responseMessage) {
                // Skip empty responses.
                response.add_part_counts(0);
                continue;
            }

            response.add_part_counts(responseMessage.Size());
            response.Attachments().insert(
                response.Attachments().end(),
                responseMessage.Begin(),
                responseMessage.End());
        }

        Reply(TError());
    }

    void Reply(const TError& error)
    {
        // Make sure that we only reply once.
        if (!AtomicTryLock(&ReplyLock))
            return;

        Awaiter->Cancel();
        Awaiter.Reset();

        Context->Reply(error);
    }

    bool CheckPrerequisites()
    {       
        auto& request = Context->Request();

        FOREACH (const auto& prerequisite, request.prerequisite_transactions()) {
            auto transactionId = FromProto<TTransactionId>(prerequisite.transaction_id());
            if (!GetPrerequisiteTransaction(transactionId)) {
                return false;
            }
        }

        auto cypressManager = Bootstrap->GetCypressManager();
        FOREACH (const auto& prerequisite, request.prerequisite_revisions()) {
            auto transactionId = FromProto<TTransactionId>(prerequisite.transaction_id());
            auto path = TYPath(prerequisite.path());
            i64 revision = prerequisite.revision();

            TTransaction* transaction = nullptr;
            if (transactionId != NullTransactionId) {
                if (!GetPrerequisiteTransaction(transactionId, &transaction)) {
                    return false;
                }
            }

            auto resolver = cypressManager->CreateResolver(transaction);
            INodePtr nodeProxy;
            try {
                nodeProxy = resolver->ResolvePath(path);
            } catch (const std::exception& ex) {
                Reply(TError(
                    NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                    "Prerequisite check failed: failed to resolve path %s",
                    ~path)
                    << ex);
                return false;
            }

            auto* cypressNodeProxy = dynamic_cast<ICypressNodeProxy*>(~nodeProxy);
            YCHECK(cypressNodeProxy);

            auto* node = cypressNodeProxy->GetTrunkNode();
            if (node->GetRevision() != revision) {
                Reply(TError(
                    NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                    "Prerequisite check failed: node %s revision mismatch: expected %" PRId64 ", found %" PRId64,
                    ~path,
                    revision,
                    node->GetRevision()));
                return false;
            }
        }

        return true;
    }

    bool GetPrerequisiteTransaction(const TTransactionId& transactionId, TTransaction** transaction = nullptr)
    {
        auto transactionManager = Bootstrap->GetTransactionManager();
        auto* myTransaction = transactionManager->FindTransaction(transactionId);
        if (!IsObjectAlive(myTransaction)) {
            Reply(TError(
                NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                "Prerequisite check failed: transaction %s is missing",
                ~ToString(transactionId)));
            return false;
        }
        if (myTransaction->GetState() != ETransactionState::Active) {
            Reply(TError(
                NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                "Prerequisite check failed: transaction %s is not active",
                ~ToString(transactionId)));
            return false;
        }
        if (transaction) {
            *transaction = myTransaction;
        }
        return true;
    }

    TUser* GetAuthenticatedUser()
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        if (!UserName) {
            return securityManager->GetRootUser();
        }

        auto* user = securityManager->FindUserByName(*UserName);
        if (!IsObjectAlive(user)) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::AuthenticationError,
                "No such user %s", ~UserName->Quote());
        }

        return user;
    }
};

////////////////////////////////////////////////////////////////////////////////

TObjectService::TObjectService(
    TObjectManagerConfigPtr config,
    TBootstrap* bootstrap)
    : TMetaStateServiceBase(
        bootstrap,
        NObjectClient::TObjectServiceProxy::GetServiceName(),
        ObjectServerLogger.GetCategory())
    , Config(config)
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GCCollect));
}

DEFINE_RPC_SERVICE_METHOD(TObjectService, Execute)
{
    UNUSED(request);
    UNUSED(response);

    New<TExecuteSession>(Bootstrap, Config, std::move(context))->Run();
}

DEFINE_RPC_SERVICE_METHOD(TObjectService, GCCollect)
{
    UNUSED(request);
    UNUSED(response);

    Bootstrap->GetObjectManager()->GCCollect().Subscribe(BIND([=] () {
        context->Reply();
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
