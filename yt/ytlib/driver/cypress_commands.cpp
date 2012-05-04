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

TCommandDescriptor TGetCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Node);
}

void TGetCommand::DoExecute(TGetRequestPtr request)
{
    TObjectServiceProxy proxy(Host->GetMasterChannel());
    auto ypathRequest = TYPathProxy::Get(WithTransaction(
        request->Path,
        Host->GetTransactionId(request)));

    ypathRequest->Attributes().MergeFrom(~request->GetOptions());
    auto ypathResponse = proxy.Execute(ypathRequest).Get();

    if (ypathResponse->IsOK()) {
        TYson value = ypathResponse->value();
        Host->ReplySuccess(value);
    } else {
        Host->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor TSetCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Node, EDataType::Null);
}

void TSetCommand::DoExecute(TSetRequestPtr request)
{
    TObjectServiceProxy proxy(Host->GetMasterChannel());
    auto ypathRequest = TYPathProxy::Set(WithTransaction(
        request->Path,
        Host->GetTransactionId(request)));

    TYson value;
    if (request->Value) {
        value = SerializeToYson(~request->Value);
    } else {
        auto producer = Host->CreateInputProducer();
        value = SerializeToYson(producer);
    }
    ypathRequest->set_value(value);

    ypathRequest->Attributes().MergeFrom(~request->GetOptions());
    auto ypathResponse = proxy.Execute(ypathRequest).Get();

    if (ypathResponse->IsOK()) {
        Host->ReplySuccess();
    } else {
        Host->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor TRemoveCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Null);
}

void TRemoveCommand::DoExecute(TRemoveRequestPtr request)
{
    TObjectServiceProxy proxy(Host->GetMasterChannel());
    auto ypathRequest = TYPathProxy::Remove(WithTransaction(
        request->Path,
        Host->GetTransactionId(request)));

    ypathRequest->Attributes().MergeFrom(~request->GetOptions());
    auto ypathResponse = proxy.Execute(ypathRequest).Get();

    if (ypathResponse->IsOK()) {
        Host->ReplySuccess();
    } else {
        Host->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor TListCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Node);
}

void TListCommand::DoExecute(TListRequestPtr request)
{
    TObjectServiceProxy proxy(Host->GetMasterChannel());
    auto ypathRequest = TYPathProxy::List(WithTransaction(
        request->Path,
        Host->GetTransactionId(request)));

    ypathRequest->Attributes().MergeFrom(~request->GetOptions());
    auto ypathResponse = proxy.Execute(ypathRequest).Get();

    if (ypathResponse->IsOK()) {
         auto consumer = Host->CreateOutputConsumer();
         BuildYsonFluently(~consumer)
            .List(ypathResponse->keys());
    } else {
        Host->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor TCreateCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Node);
}

void TCreateCommand::DoExecute(TCreateRequestPtr request)
{
    TObjectServiceProxy proxy(Host->GetMasterChannel());
    auto ypathRequest = TCypressYPathProxy::Create(WithTransaction(
        request->Path,
        Host->GetTransactionId(request)));

    ypathRequest->set_type(request->Type);

    ypathRequest->Attributes().MergeFrom(~request->GetOptions());
    auto ypathResponse = proxy.Execute(ypathRequest).Get();

    if (ypathResponse->IsOK()) {
        auto consumer = Host->CreateOutputConsumer();
        auto nodeId = TNodeId::FromProto(ypathResponse->object_id());
        BuildYsonFluently(~consumer)
            .Scalar(nodeId.ToString());
    } else {
        Host->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor TLockCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Node);
}

void TLockCommand::DoExecute(TLockRequestPtr request)
{
    TObjectServiceProxy proxy(Host->GetMasterChannel());
    auto ypathRequest = TCypressYPathProxy::Lock(WithTransaction(
        request->Path,
        Host->GetTransactionId(request)));

    ypathRequest->set_mode(request->Mode);

    ypathRequest->Attributes().MergeFrom(~request->GetOptions());
    auto ypathResponse = proxy.Execute(ypathRequest).Get();

    if (ypathResponse->IsOK()) {
        auto lockId = TLockId::FromProto(ypathResponse->lock_id());
        BuildYsonFluently(~Host->CreateOutputConsumer())
            .Scalar(lockId.ToString());
    } else {
        Host->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
