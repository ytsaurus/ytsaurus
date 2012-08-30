#include "stdafx.h"
#include "object_service.h"
#include "private.h"

#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/ypath_client.h>

#include <ytlib/rpc/message.h>

#include <ytlib/actions/parallel_awaiter.h>

#include <server/object_server/object_manager.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>

namespace NYT {
namespace NObjectServer {

using namespace NMetaState;
using namespace NRpc;
using namespace NBus;
using namespace NYTree;
using namespace NCypressServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ObjectServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TObjectService::TExecuteSession
    : public TIntrinsicRefCounted
{
public:
    TExecuteSession(TBootstrap* bootstrap, TCtxExecutePtr context)
        : Bootstrap(bootstrap)
        , Context(context)
        , Awaiter(New<TParallelAwaiter>())
        , UnavailableLock(0)
    { }

    void Run()
    {
        auto& request = Context->Request();

        int requestCount = request.part_counts_size();
        Context->SetRequestInfo("RequestCount: %d", requestCount);
        ResponseMessages.resize(requestCount);

        const auto& attachments = request.Attachments();
        auto rootService = Bootstrap->GetObjectManager()->GetRootService();
        auto awaiter = Awaiter;
        int requestPartIndex = 0;
        for (int requestIndex = 0; requestIndex < request.part_counts_size(); ++requestIndex) {
            int partCount = request.part_counts(requestIndex);
            if (partCount == 0) {
                // Skip empty requests.
                continue;
            }

            std::vector<TSharedRef> requestParts(
                attachments.begin() + requestPartIndex,
                attachments.begin() + requestPartIndex + partCount);
            auto requestMessage = CreateMessageFromParts(MoveRV(requestParts));

            NRpc::NProto::TRequestHeader requestHeader;
            if (!ParseRequestHeader(requestMessage, &requestHeader)) {
                Context->Reply(TError("Error parsing request header"));
                return;
            }

            TYPath path = requestHeader.path();
            Stroka verb = requestHeader.verb();

            LOG_DEBUG("Execute[%d] <- Path: %s, Verb: %s",
                requestIndex,
                ~path,
                ~verb);

            awaiter->Await(
                ExecuteVerb(rootService, requestMessage),
                BIND(&TExecuteSession::OnResponse, MakeStrong(this), requestIndex));

            requestPartIndex += partCount;
        }

        awaiter->Complete(BIND(&TExecuteSession::OnComplete, MakeStrong(this)));
    }

private:
    TBootstrap* Bootstrap;
    TCtxExecutePtr Context;

    TParallelAwaiterPtr Awaiter;
    std::vector<IMessagePtr> ResponseMessages;
    TAtomic UnavailableLock;

    void OnResponse(int requestIndex, IMessagePtr responseMessage)
    {
        NRpc::NProto::TResponseHeader responseHeader;
        YCHECK(ParseResponseHeader(responseMessage, &responseHeader));

        auto error = FromProto(responseHeader.error());

        LOG_DEBUG("Execute[%d] -> Error: %s",
            requestIndex,
            ~ToString(error));

        if (error.GetCode() == EErrorCode::Unavailable) {
            // OnResponse can be called from an arbitrary thread.
            // Make sure that we only reply once.
            if (AtomicTryLock(&UnavailableLock)) {
                Awaiter->Cancel();
                Awaiter.Reset();
                Context->Reply(error);
            }
        } else {
            // No sync is needed, requestIndexes are distinct.
            ResponseMessages[requestIndex] = responseMessage;
        }
    }

    void OnComplete()
    {
        // No sync is needed: OnComplete is called after all OnResponses.

        Awaiter.Reset();

        auto& response = Context->Response();

        FOREACH (const auto& responseMessage, ResponseMessages) {
            if (!responseMessage) {
                // Skip empty responses.
                response.add_part_counts(0);
                continue;
            }

            const auto& responseParts = responseMessage->GetParts();
            response.add_part_counts(static_cast<int>(responseParts.size()));
            response.Attachments().insert(
                response.Attachments().end(),
                responseParts.begin(),
                responseParts.end());
        }

        Context->Reply();
    }

};

////////////////////////////////////////////////////////////////////////////////

TObjectService::TObjectService(TBootstrap* bootstrap)
    : TMetaStateServiceBase(
        bootstrap,
        NObjectClient::TObjectServiceProxy::GetServiceName(),
        ObjectServerLogger.GetCategory())
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute));
}

DEFINE_RPC_SERVICE_METHOD(TObjectService, Execute)
{
    UNUSED(request);
    UNUSED(response);

    New<TExecuteSession>(Bootstrap, context)->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
