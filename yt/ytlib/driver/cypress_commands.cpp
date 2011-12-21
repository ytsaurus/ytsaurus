#include "stdafx.h"
#include "cypress_commands.h"

#include "../cypress/cypress_service_rpc.h"
#include "../ytree/ypath_rpc.h"
#include "../ytree/serialize.h"

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NCypress;

////////////////////////////////////////////////////////////////////////////////

void TGetCommand::DoExecute(TGetRequest* request)
{
    TCypressServiceProxy proxy(DriverImpl->GetMasterChannel());
    auto ypathRequest = TYPathProxy::Get();
    auto ypathResponse = proxy.Execute(
        request->Path,
        DriverImpl->GetCurrentTransactionId(),
        ~ypathRequest)->Get();

    if (ypathResponse->IsOK()) {
        TYson value = ypathResponse->value();
        DriverImpl->ReplySuccess(ypathResponse->value(), request->Stream);
    } else {
        DriverImpl->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TSetCommand::DoExecute(TSetRequest* request)
{
    TCypressServiceProxy proxy(DriverImpl->GetMasterChannel());
    auto ypathRequest = TYPathProxy::Set();

    TYson value;
    if (request->Value) {
        value = SerializeToYson(~request->Value);
    } else {
        auto producer = DriverImpl->CreateInputProducer(request->Stream);
        value = SerializeToYson(~producer);
    }
    ypathRequest->set_value(value);

    auto ypathResponse = proxy.Execute(
        request->Path,
        DriverImpl->GetCurrentTransactionId(),
        ~ypathRequest)->Get();

    if (!ypathResponse->IsOK()) {
        DriverImpl->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
