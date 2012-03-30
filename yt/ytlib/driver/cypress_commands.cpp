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
    TCypressServiceProxy proxy(Host->GetMasterChannel());
    auto ypathRequest = TYPathProxy::Get(WithTransaction(
        Host->PreprocessYPath(request->Path),
        Host->GetTransactionId(request)));
    ypathRequest->Attributes().MergeFrom(~request->GetOptions());
    auto ypathResponse = proxy.Execute(~ypathRequest)->Get();

    if (ypathResponse->IsOK()) {
        TYson value = ypathResponse->value();
        Host->ReplySuccess(value);
    } else {
        Host->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TSetCommand::DoExecute(TSetRequest* request)
{
    TCypressServiceProxy proxy(Host->GetMasterChannel());

    auto ypathRequest = TYPathProxy::Set(WithTransaction(
        Host->PreprocessYPath(request->Path),
        Host->GetTransactionId(request)));
    ypathRequest->Attributes().MergeFrom(~request->GetOptions());

    TYson value;
    if (request->Value) {
        value = SerializeToYson(~request->Value);
    } else {
        auto producer = Host->CreateInputProducer();
        value = SerializeToYson(producer);
    }
    ypathRequest->set_value(value);

    auto ypathResponse = proxy.Execute(~ypathRequest)->Get();

    if (ypathResponse->IsOK()) {
        Host->ReplySuccess();
    } else {
        Host->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveCommand::DoExecute(TRemoveRequest* request)
{
    TCypressServiceProxy proxy(Host->GetMasterChannel());
    auto ypathRequest = TYPathProxy::Remove(WithTransaction(
        Host->PreprocessYPath(request->Path),
        Host->GetTransactionId(request)));

    auto ypathResponse = proxy.Execute(~ypathRequest)->Get();
    ypathRequest->Attributes().MergeFrom(~request->GetOptions());

    if (ypathResponse->IsOK()) {
        Host->ReplySuccess();
    } else {
        Host->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TListCommand::DoExecute(TListRequest* request)
{
    TCypressServiceProxy proxy(Host->GetMasterChannel());
    auto ypathRequest = TYPathProxy::List(WithTransaction(
        Host->PreprocessYPath(request->Path),
        Host->GetTransactionId(request)));

    auto ypathResponse = proxy.Execute(~ypathRequest)->Get();
    ypathRequest->Attributes().MergeFrom(~request->GetOptions());

    if (ypathResponse->IsOK()) {
         auto consumer = Host->CreateOutputConsumer();
         BuildYsonFluently(~consumer)
             .DoListFor(ypathResponse->keys(), [=] (TFluentList fluent, Stroka key)
                {
                    fluent.Item().Scalar(key);
                });
    } else {
        Host->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TCreateCommand::DoExecute(TCreateRequest* request)
{
    TCypressServiceProxy proxy(Host->GetMasterChannel());
    auto ypathRequest = TCypressYPathProxy::Create(WithTransaction(
        Host->PreprocessYPath(request->Path),
        Host->GetTransactionId(request)));

    ypathRequest->set_type(request->Type);

    if (request->Manifest) {
        auto serializedManifest = SerializeToYson(~request->Manifest, EYsonFormat::Binary);
        ypathRequest->set_manifest(serializedManifest);
    }

    auto ypathResponse = proxy.Execute(~ypathRequest)->Get();
    ypathRequest->Attributes().MergeFrom(~request->GetOptions());

    if (ypathResponse->IsOK()) {
        auto consumer = Host->CreateOutputConsumer();
        auto id = TNodeId::FromProto(ypathResponse->object_id());
        BuildYsonFluently(~consumer)
            .BeginMap()
                .Item("object_id").Scalar(id.ToString())
            .EndMap();
    } else {
        Host->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TLockCommand::DoExecute(TLockRequest* request)
{
    TCypressServiceProxy proxy(Host->GetMasterChannel());
    auto ypathRequest = TCypressYPathProxy::Lock(WithTransaction(
        Host->PreprocessYPath(request->Path),
        Host->GetTransactionId(request)));
    ypathRequest->set_mode(request->Mode);

    auto ypathResponse = proxy.Execute(~ypathRequest)->Get();
    ypathRequest->Attributes().MergeFrom(~request->GetOptions());

    if (ypathResponse->IsOK()) {
        auto lockId = TLockId::FromProto(ypathResponse->lock_id());
        BuildYsonFluently(~Host->CreateOutputConsumer())
            .BeginMap()
                .Item("lock_id").Scalar(lockId.ToString())
            .EndMap();
    } else {
        Host->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
