#include "stdafx.h"
#include "cypress_commands.h"

#include "../cypress/cypress_service_rpc.h"
#include "../cypress/cypress_ypath_rpc.h"
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
        request->Path,
        DriverImpl->GetCurrentTransactionId(),
        ~ypathRequest)->Get();

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
        request->Path,
        DriverImpl->GetCurrentTransactionId(),
        ~ypathRequest)->Get();

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
        request->Path,
        DriverImpl->GetCurrentTransactionId(),
        ~ypathRequest)->Get();

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

    auto serializedManifest = SerializeToYson(~request->Manifest, TYsonWriter::EFormat::Binary);
    ypathRequest->set_manifest(serializedManifest);

    auto ypathResponse = proxy.Execute(
        request->Path,
        DriverImpl->GetCurrentTransactionId(),
        ~ypathRequest)->Get();

    if (ypathResponse->IsOK()) {
        auto consumer = DriverImpl->CreateOutputConsumer(ToStreamSpec(request->Stream));
        auto nodeId = TNodeId::FromProto(ypathResponse->nodeid());
        BuildYsonFluently(~consumer)
            .BeginMap()
                .Item("node_id").Scalar(nodeId.ToString())
            .EndMap();
    } else {
        DriverImpl->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
