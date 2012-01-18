#include "stdafx.h"
#include "cypress_service.h"
#include "node_proxy.h"

#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/rpc/message.h>

namespace NYT {
namespace NCypress {

using namespace NRpc;
using namespace NBus;
using namespace NYTree;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = CypressLogger;

////////////////////////////////////////////////////////////////////////////////

TCypressService::TCypressService(
    IInvoker* invoker,
    TCypressManager* cypressManager)
    : NRpc::TServiceBase(
        invoker,
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

        LOG_DEBUG("Execute: RequestIndex: %d, Path: %s, Verb: %s",
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

                LOG_DEBUG("Execute: RequestIndex: %d, PartCount: %d, Error: %s",
                    requestIndex,
                    responseParts.ysize(),
                    ~error.ToString());

                response->add_part_counts(responseParts.ysize());
                response->Attachments().insert(
                    response->Attachments().end(),
                    responseParts.begin(),
                    responseParts.end());

                if (requestIndex == requestCount) {
                    context->Reply();
                }
            }));

        currentIndex += partCount;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
