#include "stdafx.h"
#include "cypress_commands.h"

#include "../cypress/cypress_service_proxy.h"
#include "../cypress/cypress_ypath_proxy.h"
#include "../ytree/ypath_proxy.h"
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
    auto optionsNode = request->GetOptions();
    ypathRequest->set_options(SerializeToYson(~optionsNode));
    auto ypathResponse = proxy.Execute(
        ~ypathRequest,
        request->Path,
        DriverImpl->GetCurrentTransactionId())->Get();

    if (ypathResponse->IsOK()) {
        TYson value = ypathResponse->value();
        DriverImpl->ReplySuccess(value, ToStreamSpec(request->Stream));
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
        auto producer = DriverImpl->CreateInputProducer(ToStreamSpec(request->Stream));
        value = SerializeToYson(~producer);
    }
    ypathRequest->set_value(value);

    auto ypathResponse = proxy.Execute(
        ~ypathRequest,
        request->Path,
        DriverImpl->GetCurrentTransactionId())->Get();

    if (!ypathResponse->IsOK()) {
        DriverImpl->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveCommand::DoExecute(TRemoveRequest* request)
{
    TCypressServiceProxy proxy(DriverImpl->GetMasterChannel());
    auto ypathRequest = TYPathProxy::Remove();

    auto ypathResponse = proxy.Execute(
        ~ypathRequest,
        request->Path,
        DriverImpl->GetCurrentTransactionId())->Get();

    if (!ypathResponse->IsOK()) {
        DriverImpl->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TListCommand::DoExecute(TListRequest* request)
{
    TCypressServiceProxy proxy(DriverImpl->GetMasterChannel());
    auto ypathRequest = TYPathProxy::List();

    auto ypathResponse = proxy.Execute(
        ~ypathRequest,
        request->Path,
        DriverImpl->GetCurrentTransactionId())->Get();

    if (ypathResponse->IsOK()) {
         auto consumer = DriverImpl->CreateOutputConsumer(ToStreamSpec(request->Stream));
         BuildYsonFluently(~consumer)
             .DoListFor(ypathResponse->keys(), [=] (TFluentList fluent, Stroka key)
                {
                    fluent.Item().Scalar(key);
                });
    } else {
        DriverImpl->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TCreateCommand::DoExecute(TCreateRequest* request)
{
    TCypressServiceProxy proxy(DriverImpl->GetMasterChannel());
    auto ypathRequest = TCypressYPathProxy::Create();

    ypathRequest->set_type(request->Type);

    if (request->Manifest) {
        auto serializedManifest = SerializeToYson(~request->Manifest, TYsonWriter::EFormat::Binary);
        ypathRequest->set_manifest(serializedManifest);
    }

    auto ypathResponse = proxy.Execute(
        ~ypathRequest,
        request->Path,
        DriverImpl->GetCurrentTransactionId())->Get();

    if (!ypathResponse->IsOK()) {
        DriverImpl->ReplyError(ypathResponse->GetError());
        return;
    }

    auto consumer = DriverImpl->CreateOutputConsumer(ToStreamSpec(request->Stream));
    auto id = TNodeId::FromProto(ypathResponse->id());
    BuildYsonFluently(~consumer)
        .BeginMap()
            .Item("id").Scalar(id.ToString())
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
