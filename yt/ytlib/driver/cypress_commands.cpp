#include "stdafx.h"
#include "cypress_commands.h"

#include <ytlib/cypress/cypress_service_proxy.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/ytree/serialize.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NCypress;

////////////////////////////////////////////////////////////////////////////////

void TGetCommand::DoExecute(TGetRequest* request)
{
    TCypressServiceProxy proxy(DriverImpl->GetMasterChannel());
    auto ypathRequest = TYPathProxy::Get(WithTransaction(
        request->Path,
        DriverImpl->GetTransactionId(request)));
    ypathRequest->Attributes().MergeFrom(~request->GetOptions());
    auto ypathResponse = proxy.Execute(ypathRequest)->Get();

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

    auto ypathRequest = TYPathProxy::Set(WithTransaction(
        request->Path,
        DriverImpl->GetTransactionId(request)));
    ypathRequest->Attributes().MergeFrom(~request->GetOptions());

    TYson value;
    if (request->Value) {
        value = SerializeToYson(~request->Value);
    } else {
        auto producer = DriverImpl->CreateInputProducer(ToStreamSpec(request->Stream));
        value = SerializeToYson(producer);
    }
    ypathRequest->set_value(value);

    auto ypathResponse = proxy.Execute(ypathRequest)->Get();

    if (ypathResponse->IsOK()) {
        DriverImpl->ReplySuccess();
    } else {
        DriverImpl->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveCommand::DoExecute(TRemoveRequest* request)
{
    TCypressServiceProxy proxy(DriverImpl->GetMasterChannel());
    auto ypathRequest = TYPathProxy::Remove(WithTransaction(
        request->Path,
        DriverImpl->GetTransactionId(request)));

    auto ypathResponse = proxy.Execute(ypathRequest)->Get();
    ypathRequest->Attributes().MergeFrom(~request->GetOptions());

    if (ypathResponse->IsOK()) {
        DriverImpl->ReplySuccess();
    } else {
        DriverImpl->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TListCommand::DoExecute(TListRequest* request)
{
    TCypressServiceProxy proxy(DriverImpl->GetMasterChannel());
    auto ypathRequest = TYPathProxy::List(WithTransaction(
        request->Path,
        DriverImpl->GetTransactionId(request)));

    auto ypathResponse = proxy.Execute(ypathRequest)->Get();
    ypathRequest->Attributes().MergeFrom(~request->GetOptions());

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
    auto ypathRequest = TCypressYPathProxy::Create(WithTransaction(
        request->Path,
        DriverImpl->GetTransactionId(request)));

    ypathRequest->set_type(request->Type);

    if (request->Manifest) {
        auto serializedManifest = SerializeToYson(~request->Manifest, EYsonFormat::Binary);
        ypathRequest->set_manifest(serializedManifest);
    }

    auto ypathResponse = proxy.Execute(ypathRequest)->Get();
    ypathRequest->Attributes().MergeFrom(~request->GetOptions());

    if (ypathResponse->IsOK()) {
        auto consumer = DriverImpl->CreateOutputConsumer(ToStreamSpec(request->Stream));
        auto id = TNodeId::FromProto(ypathResponse->object_id());
        BuildYsonFluently(~consumer)
            .BeginMap()
                .Item("object_id").Scalar(id.ToString())
            .EndMap();
    } else {
        DriverImpl->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TLockCommand::DoExecute(TLockRequest* request)
{
    TCypressServiceProxy proxy(DriverImpl->GetMasterChannel());
    auto ypathRequest = TCypressYPathProxy::Lock(WithTransaction(
        request->Path,
        DriverImpl->GetTransactionId(request)));
    ypathRequest->set_mode(request->Mode);

    auto ypathResponse = proxy.Execute(ypathRequest)->Get();
    ypathRequest->Attributes().MergeFrom(~request->GetOptions());

    if (ypathResponse->IsOK()) {
        auto lockId = TLockId::FromProto(ypathResponse->lock_id());
        BuildYsonFluently(~DriverImpl->CreateOutputConsumer())
            .BeginMap()
                .Item("lock_id").Scalar(lockId.ToString())
            .EndMap();
    } else {
        DriverImpl->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
