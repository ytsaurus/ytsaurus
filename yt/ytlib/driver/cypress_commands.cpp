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
    TCypressServiceProxy proxy(~DriverImpl->GetCellChannel());
    auto ypathRequest = TYPathProxy::Get();
    auto ypathResponse = proxy.Execute(request->Path, DriverImpl->GetTransactionId(), ~ypathRequest)->Get();

    if (ypathResponse->IsOK()) {
        ReplySuccess(ypathResponse->value());
    } else {
        ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TSetCommand::DoExecute(TSetRequest* request)
{
    TCypressServiceProxy proxy(~DriverImpl->GetCellChannel());
    auto ypathRequest = TYPathProxy::Set();
    ypathRequest->set_value(SerializeToYson(~request->Value));
    auto ypathResponse = proxy.Execute(request->Path, DriverImpl->GetTransactionId(), ~ypathRequest)->Get();

    if (ypathResponse->IsOK()) {
        ReplySuccess();
    } else {
        ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
