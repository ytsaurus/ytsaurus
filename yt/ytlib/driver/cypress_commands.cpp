#include "stdafx.h"
#include "cypress_commands.h"

#include <ytlib/object_server/object_service_proxy.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/ytree/serialize.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NCypress;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

void TGetCommand::DoExecute()
{
    TObjectServiceProxy proxy(Context->GetMasterChannel());
    auto req = TYPathProxy::Get(WithTransaction(
        Request->Path,
        GetTransactionId(false)));

    req->Attributes().MergeFrom(Request->GetOptions());
    auto rsp = proxy.Execute(req).Get();

    if (!rsp->IsOK()) {
        ReplyError(rsp->GetError());
        return;
    }

    ReplySuccess(rsp->value());
}

////////////////////////////////////////////////////////////////////////////////
/*
TCommandDescriptor TSetCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Node, EDataType::Null);
}

void TSetCommand::DoExecute(TSetRequestPtr request)
{
    TObjectServiceProxy proxy(Context->GetMasterChannel());
    auto ypathRequest = TYPathProxy::Set(WithTransaction(
        request->Path,
        Context->GetTransactionId(request)));

    TYson value;
    if (request->Value) {
        value = SerializeToYson(~request->Value);
    } else {
        auto producer = Context->CreateInputProducer();
        value = SerializeToYson(producer);
    }
    ypathRequest->set_value(value);

    ypathRequest->Attributes().MergeFrom(~request->GetOptions());
    auto ypathResponse = proxy.Execute(ypathRequest).Get();

    if (ypathResponse->IsOK()) {
        Context->ReplySuccess();
    } else {
        Context->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor TRemoveCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Null);
}

void TRemoveCommand::DoExecute(TRemoveRequestPtr request)
{
    TObjectServiceProxy proxy(Context->GetMasterChannel());
    auto ypathRequest = TYPathProxy::Remove(WithTransaction(
        request->Path,
        Context->GetTransactionId(request)));

    ypathRequest->Attributes().MergeFrom(~request->GetOptions());
    auto ypathResponse = proxy.Execute(ypathRequest).Get();

    if (ypathResponse->IsOK()) {
        Context->ReplySuccess();
    } else {
        Context->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor TListCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Node);
}

void TListCommand::DoExecute(TListRequestPtr request)
{
    TObjectServiceProxy proxy(Context->GetMasterChannel());
    auto ypathRequest = TYPathProxy::List(WithTransaction(
        request->Path,
        Context->GetTransactionId(request)));

    ypathRequest->Attributes().MergeFrom(~request->GetOptions());
    auto ypathResponse = proxy.Execute(ypathRequest).Get();

    if (ypathResponse->IsOK()) {
         auto consumer = Context->CreateOutputConsumer();
         BuildYsonFluently(~consumer)
            .List(ypathResponse->keys());
    } else {
        Context->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor TCreateCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Node);
}

void TCreateCommand::DoExecute(TCreateRequestPtr request)
{
    TObjectServiceProxy proxy(Context->GetMasterChannel());
    auto ypathRequest = TCypressYPathProxy::Create(WithTransaction(
        request->Path,
        Context->GetTransactionId(request)));

    ypathRequest->set_type(request->Type);

    ypathRequest->Attributes().MergeFrom(~request->GetOptions());
    auto ypathResponse = proxy.Execute(ypathRequest).Get();

    if (ypathResponse->IsOK()) {
        auto consumer = Context->CreateOutputConsumer();
        auto nodeId = TNodeId::FromProto(ypathResponse->object_id());
        BuildYsonFluently(~consumer)
            .Scalar(nodeId.ToString());
    } else {
        Context->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor TLockCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Node);
}

void TLockCommand::DoExecute(TLockRequestPtr request)
{
    TObjectServiceProxy proxy(Context->GetMasterChannel());
    auto ypathRequest = TCypressYPathProxy::Lock(WithTransaction(
        request->Path,
        Context->GetTransactionId(request)));

    ypathRequest->set_mode(request->Mode);

    ypathRequest->Attributes().MergeFrom(~request->GetOptions());
    auto ypathResponse = proxy.Execute(ypathRequest).Get();

    if (ypathResponse->IsOK()) {
        auto lockId = TLockId::FromProto(ypathResponse->lock_id());
        BuildYsonFluently(~Context->CreateOutputConsumer())
            .Scalar(lockId.ToString());
    } else {
        Context->ReplyError(ypathResponse->GetError());
    }
}
*/
////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
