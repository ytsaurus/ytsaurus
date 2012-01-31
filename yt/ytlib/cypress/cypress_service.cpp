#include "stdafx.h"
#include "cypress_service.h"

#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/rpc/message.h>

namespace NYT {
namespace NCypress {

using namespace NMetaState;
using namespace NRpc;
using namespace NBus;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = CypressLogger;

////////////////////////////////////////////////////////////////////////////////

TCypressService::TCypressService(
    NMetaState::IMetaStateManager* metaStateManager,
    TCypressManager* cypressManager)
    : TMetaStateServiceBase(
        metaStateManager,
        TCypressServiceProxy::GetServiceName(),
        CypressLogger.GetCategory())
    , CypressManager(cypressManager)
{
    YASSERT(cypressManager);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TCypressService, Execute)
{
    UNUSED(response);

    int requestCount = request->part_counts_size();
    context->SetRequestInfo("RequestCount: %d", requestCount);

    ValidateLeader();

    const auto& attachments = request->Attachments();

    int currentIndex = 0;
    for (int requestIndex = 0; requestIndex < request->part_counts_size(); ++requestIndex) {
        int partCount = request->part_counts(requestIndex);
        YASSERT(partCount >= 2);
        yvector<TSharedRef> requestParts(
            attachments.begin() + currentIndex,
            attachments.begin() + currentIndex + partCount);
        auto requestMessage = CreateMessageFromParts(MoveRV(requestParts));

        auto requestHeader = GetRequestHeader(~requestMessage);
        TYPath path = requestHeader.path();
        Stroka verb = requestHeader.verb();

        LOG_DEBUG("Execute[%d]: Path: %s, Verb: %s",
            requestIndex,
            ~path,
            ~verb);

        auto processor = CypressManager->CreateProcessor();

        ExecuteVerb(~requestMessage, ~processor)
        ->Subscribe(FromFunctor([=] (IMessage::TPtr responseMessage)
            {
                auto responseHeader = GetResponseHeader(~responseMessage);
                const auto& responseParts = responseMessage->GetParts();
                auto error = GetResponseError(responseHeader);

                LOG_DEBUG("Execute[%d]: Error: %s",
                    requestIndex,
                    ~error.ToString());

                response->add_part_counts(responseParts.ysize());
                response->Attachments().insert(
                    response->Attachments().end(),
                    responseParts.begin(),
                    responseParts.end());

                if (requestIndex == requestCount - 1) {
                    context->Reply();
                }
            }));

        currentIndex += partCount;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
