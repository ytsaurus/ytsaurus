#include "stdafx.h"
#include "cypress_service.h"

#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/rpc/message.h>
#include <ytlib/actions/parallel_awaiter.h>

namespace NYT {
namespace NCypress {

using namespace NMetaState;
using namespace NRpc;
using namespace NBus;
using namespace NYTree;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = CypressLogger;

////////////////////////////////////////////////////////////////////////////////

class TCypressService::TExecuteSession
    : public TIntrinsicRefCounted
{
public:
    typedef TIntrusivePtr<TExecuteSession> TPtr;

    TExecuteSession(TCypressService* owner, TCtxExecute* context)
        : Context(context)
        , Owner(owner)
    { }

    void Execute()
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
            YASSERT(partCount >= 2);
            yvector<TSharedRef> requestParts(
                attachments.begin() + requestPartIndex,
                attachments.begin() + requestPartIndex + partCount);
            auto requestMessage = CreateMessageFromParts(MoveRV(requestParts));

            auto requestHeader = GetRequestHeader(~requestMessage);
            TYPath path = requestHeader.path();
            Stroka verb = requestHeader.verb();

            LOG_DEBUG("Execute[%d] <- Path: %s, Verb: %s",
                requestIndex,
                ~path,
                ~verb);

            auto service = Owner->ObjectManager->GetRootService();

            awaiter->Await(
                ExecuteVerb(service, ~requestMessage),
                FromMethod(&TExecuteSession::OnResponse, TPtr(this), requestIndex));

            requestPartIndex += partCount;
        }

        awaiter->Complete(FromMethod(&TExecuteSession::OnComplete, TPtr(this)));
    }

private:
    TCtxExecute::TPtr Context;
    TCypressService::TPtr Owner;
    std::vector<IMessage::TPtr> ResponseMessages;

    void OnResponse(IMessage::TPtr responseMessage, int requestIndex)
    {
        auto responseHeader = GetResponseHeader(~responseMessage);
        auto error = GetResponseError(responseHeader);

        LOG_DEBUG("Execute[%d] -> Error: %s",
            requestIndex,
            ~error.ToString());

        ResponseMessages[requestIndex] = responseMessage;
    }

    void OnComplete()
    {
        auto& response = Context->Response();

        FOREACH (const auto& responseMessage, ResponseMessages) {
            const auto& responseParts = responseMessage->GetParts();
            response.add_part_counts(responseParts.ysize());
            response.Attachments().insert(
                response.Attachments().end(),
                responseParts.begin(),
                responseParts.end());
        }

        Context->Reply();
    }

};

////////////////////////////////////////////////////////////////////////////////

TCypressService::TCypressService(
    NMetaState::IMetaStateManager* metaStateManager,
    TObjectManager* objectManager)
    : TMetaStateServiceBase(
        metaStateManager,
        TCypressServiceProxy::GetServiceName(),
        CypressLogger.GetCategory())
    , ObjectManager(objectManager)
{
    YASSERT(objectManager);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute));
}

DEFINE_RPC_SERVICE_METHOD(TCypressService, Execute)
{
    UNUSED(request);
    UNUSED(response);

    ValidateLeader();

    auto session = New<TExecuteSession>(this, ~context);
    session->Execute();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
