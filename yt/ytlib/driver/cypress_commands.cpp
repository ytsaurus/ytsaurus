#include "stdafx.h"
#include "cypress_commands.h"

#include <ytlib/object_server/object_service_proxy.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/ytree/ypath_proxy.h>

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

    ReplySuccess(TYsonString(rsp->value()));
}

////////////////////////////////////////////////////////////////////////////////

void TSetCommand::DoExecute()
{
    TObjectServiceProxy proxy(Context->GetMasterChannel());
    auto req = TYPathProxy::Set(WithTransaction(
        Request->Path,
        GetTransactionId(false)));

    auto producer = Context->CreateInputProducer();
    TYsonString value = ConvertToYsonString(producer);
    req->set_value(value.Data());

    req->Attributes().MergeFrom(Request->GetOptions());
    auto rsp = proxy.Execute(req).Get();

    if (!rsp->IsOK()) {
        ReplyError(rsp->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveCommand::DoExecute()
{
    TObjectServiceProxy proxy(Context->GetMasterChannel());
    auto req = TYPathProxy::Remove(WithTransaction(
        Request->Path,
        GetTransactionId(false)));

    req->Attributes().MergeFrom(Request->GetOptions());
    auto rsp = proxy.Execute(req).Get();

    if (!rsp->IsOK()) {
        ReplyError(rsp->GetError());
    }
}

//////////////////////////////////////////////////////////////////////////////////

void TListCommand::DoExecute()
{
    TObjectServiceProxy proxy(Context->GetMasterChannel());
    auto req = TYPathProxy::List(WithTransaction(
        Request->Path,
        GetTransactionId(false)));

    req->Attributes().MergeFrom(Request->GetOptions());
    auto rsp = proxy.Execute(req).Get();

    if (rsp->IsOK()) {
         auto consumer = Context->CreateOutputConsumer();
         BuildYsonFluently(~consumer)
            .List(rsp->keys());
    } else {
        ReplyError(rsp->GetError());
    }
}

//////////////////////////////////////////////////////////////////////////////////

void TCreateCommand::DoExecute()
{
    TObjectServiceProxy proxy(Context->GetMasterChannel());
    auto req = TCypressYPathProxy::Create(WithTransaction(
        Request->Path,
        GetTransactionId(false)));

    req->set_type(Request->Type);

    req->Attributes().MergeFrom(Request->GetOptions());
    auto rsp = proxy.Execute(req).Get();

    if (rsp->IsOK()) {
        auto consumer = Context->CreateOutputConsumer();
        auto nodeId = TNodeId::FromProto(rsp->object_id());
        BuildYsonFluently(~consumer)
            .Scalar(nodeId.ToString());
    } else {
        ReplyError(rsp->GetError());
    }
}

//////////////////////////////////////////////////////////////////////////////////

void TLockCommand::DoExecute()
{
    TObjectServiceProxy proxy(Context->GetMasterChannel());
    auto req = TCypressYPathProxy::Lock(WithTransaction(
        Request->Path,
        GetTransactionId(true)));

    req->set_mode(Request->Mode);

    req->Attributes().MergeFrom(Request->GetOptions());
    auto rsp = proxy.Execute(req).Get();

    if (!rsp->IsOK()) {
        ReplyError(rsp->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
