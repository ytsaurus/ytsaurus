#include "stdafx.h"
#include "cypress_commands.h"

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/ytree/attribute_helpers.h>

#include <ytlib/meta_state/rpc_helpers.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

void TGetCommand::DoExecute()
{
    TObjectServiceProxy proxy(Context->GetMasterChannel());
    auto req = TYPathProxy::Get(Request->Path.GetPath());
    SetTransactionId(req, GetTransactionId(false));

    TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly, Request->Attributes);
    *req->mutable_attribute_filter() = ToProto(attributeFilter);

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
    auto req = TYPathProxy::Set(Request->Path.GetPath());
    SetTransactionId(req, GetTransactionId(false));
    NMetaState::GenerateRpcMutationId(req);

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
    auto req = TYPathProxy::Remove(Request->Path.GetPath());
    req->set_recursive(Request->Recursive);
    req->set_force(Request->Force);
    SetTransactionId(req, GetTransactionId(false));
    NMetaState::GenerateRpcMutationId(req);

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
    auto req = TYPathProxy::List(Request->Path.GetPath());
    SetTransactionId(req, GetTransactionId(false));

    TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly, Request->Attributes);
    *req->mutable_attribute_filter() = ToProto(attributeFilter);

    req->Attributes().MergeFrom(Request->GetOptions());
    auto rsp = proxy.Execute(req).Get();

    if (!rsp->IsOK()) {
        ReplyError(rsp->GetError());
        return;
    }

    ReplySuccess(TYsonString(rsp->keys()));
}

//////////////////////////////////////////////////////////////////////////////////

void TCreateCommand::DoExecute()
{
    TObjectServiceProxy proxy(Context->GetMasterChannel());

    if (IsTypeVersioned(Request->Type)) {
        if (!Request->Path) {
            THROW_ERROR_EXCEPTION("Object type is versioned, Cypress path required");
        }

        auto req = TCypressYPathProxy::Create(Request->Path.Get().GetPath());
        req->set_type(Request->Type);
        req->set_recursive(Request->Recursive);
        req->set_ignore_existing(Request->IgnoreExisting);
        SetTransactionId(req, GetTransactionId(false));
        NMetaState::GenerateRpcMutationId(req);

        if (Request->Attributes) {
            auto attributes = ConvertToAttributes(Request->Attributes);
            ToProto(req->mutable_node_attributes(), *attributes);
        }

        auto rsp = proxy.Execute(req).Get();
        if (!rsp->IsOK()) {
            ReplyError(rsp->GetError());
            return;
        }

        auto consumer = Context->CreateOutputConsumer();
        auto nodeId = TNodeId::FromProto(rsp->node_id());
        BuildYsonFluently(~consumer)
            .Value(nodeId);

    } else {
        if (Request->Path) {
            THROW_ERROR_EXCEPTION("Object type is nonversioned, Cypress path is not required");
        }

        auto transactionId = GetTransactionId(false);
        auto req = TTransactionYPathProxy::CreateObject(
            transactionId == NullTransactionId
            ? RootTransactionPath
            : FromObjectId(transactionId));
        req->set_type(Request->Type);
        NMetaState::GenerateRpcMutationId(req);

        if (Request->Attributes) {
            auto attributes = ConvertToAttributes(Request->Attributes);
            ToProto(req->mutable_object_attributes(), *attributes);
        }

        auto rsp = proxy.Execute(req).Get();
        if (!rsp->IsOK()) {
            ReplyError(rsp->GetError());
            return;
        }

        auto consumer = Context->CreateOutputConsumer();
        auto objectId = TNodeId::FromProto(rsp->object_id());
        BuildYsonFluently(~consumer)
            .Value(objectId);
    }
}

//////////////////////////////////////////////////////////////////////////////////

void TLockCommand::DoExecute()
{
    TObjectServiceProxy proxy(Context->GetMasterChannel());
    auto req = TCypressYPathProxy::Lock(Request->Path.GetPath());
    SetTransactionId(req, GetTransactionId(true));
    NMetaState::GenerateRpcMutationId(req);

    req->set_mode(Request->Mode);

    req->Attributes().MergeFrom(Request->GetOptions());
    auto rsp = proxy.Execute(req).Get();

    if (!rsp->IsOK()) {
        ReplyError(rsp->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TCopyCommand::DoExecute()
{
    TObjectServiceProxy proxy(Context->GetMasterChannel());
    auto req = TCypressYPathProxy::Copy(Request->DestinationPath.GetPath());
    SetTransactionId(req, GetTransactionId(false));
    NMetaState::GenerateRpcMutationId(req);
    req->set_source_path(Request->SourcePath.GetPath());

    auto rsp = proxy.Execute(req).Get();

    if (!rsp->IsOK()) {
        ReplyError(rsp->GetError());
        return;
    }

    auto consumer = Context->CreateOutputConsumer();
    auto nodeId = TNodeId::FromProto(rsp->object_id());
    BuildYsonFluently(~consumer)
        .Value(nodeId.ToString());
}

////////////////////////////////////////////////////////////////////////////////

void TMoveCommand::DoExecute()
{
    TObjectServiceProxy proxy(Context->GetMasterChannel());
    {
        auto req = TCypressYPathProxy::Copy(Request->DestinationPath.GetPath());
        SetTransactionId(req, GetTransactionId(false));
        NMetaState::GenerateRpcMutationId(req);
        req->set_source_path(Request->SourcePath.GetPath());
        auto rsp = proxy.Execute(req).Get();

        if (!rsp->IsOK()) {
            ReplyError(rsp->GetError());
            return;
        }
    }

    {
        auto req = TYPathProxy::Remove(Request->SourcePath.GetPath());
        req->set_recursive(true);
        SetTransactionId(req, GetTransactionId(false));
        NMetaState::GenerateRpcMutationId(req);

        auto rsp = proxy.Execute(req).Get();
        if (!rsp->IsOK()) {
            ReplyError(rsp->GetError());
            return;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TExistsCommand::DoExecute()
{
    TObjectServiceProxy proxy(Context->GetMasterChannel());
    auto req = TYPathProxy::Exists(Request->Path.GetPath());
    SetTransactionId(req, GetTransactionId(false));

    auto rsp = proxy.Execute(req).Get();

    if (!rsp->IsOK()) {
        ReplyError(rsp->GetError());
    }
    else {
        ReplySuccess(ConvertToYsonString(rsp->value()));
    }
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
