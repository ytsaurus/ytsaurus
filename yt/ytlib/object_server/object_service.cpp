#include "stdafx.h"
#include "object_service.h"

#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/rpc/message.h>
#include <ytlib/actions/parallel_awaiter.h>
#include <ytlib/object_server/object_manager.h>
#include <ytlib/cell_master/bootstrap.h>

namespace NYT {
namespace NObjectServer {

using namespace NMetaState;
using namespace NRpc;
using namespace NBus;
using namespace NYTree;
using namespace NCypressServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Cypress");

////////////////////////////////////////////////////////////////////////////////

class TObjectService::TExecuteSession
    : public TIntrinsicRefCounted
{
public:
    TExecuteSession(TObjectService* owner, TCtxExecutePtr context)
        : Context(context)
        , Owner(owner)
    { }

    void Run()
    {
        auto& request = Context->Request();

        int requestCount = request.part_counts_size();
        Context->SetRequestInfo("RequestCount: %d", requestCount);
        ResponseMessages.resize(requestCount);

        const auto& attachments = request.Attachments();
        int requestPartIndex = 0;
        auto awaiter = New<TParallelAwaiter>();
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

            auto rootService = Owner
                ->Bootstrap
                ->GetObjectManager()
                ->GetRootService();

            awaiter->Await(
                ExecuteVerb(rootService, requestMessage),
                BIND(&TExecuteSession::OnResponse, MakeStrong(this), requestIndex));

            requestPartIndex += partCount;
        }

        awaiter->Complete(BIND(&TExecuteSession::OnComplete, MakeStrong(this)));
    }

private:
    TCtxExecutePtr Context;
    TObjectService::TPtr Owner;
    std::vector<IMessagePtr> ResponseMessages;

    void OnResponse(int requestIndex, IMessagePtr responseMessage)
    {
        NRpc::NProto::TResponseHeader responseHeader;
        YCHECK(ParseResponseHeader(responseMessage, &responseHeader));

        auto error = TError::FromProto(responseHeader.error());

        LOG_DEBUG("Execute[%d] -> Error: %s",
            requestIndex,
            ~error.ToString());

        ResponseMessages[requestIndex] = responseMessage;
    }

    void OnComplete()
    {
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
        TObjectServiceProxy::GetServiceName(),
        Logger.GetCategory())
{
    YASSERT(bootstrap);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute));
}

DEFINE_RPC_SERVICE_METHOD(TObjectService, Execute)
{
    UNUSED(request);
    UNUSED(response);

    New<TExecuteSession>(this, context)->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
