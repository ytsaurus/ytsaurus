#include "stdafx.h"
#include "cypress_commands.h"

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>

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
    auto req = TYPathProxy::Get(Request->Path.GetPath());
    SetTransactionId(req, GetTransactionId(false));

    TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly, Request->Attributes);
    *req->mutable_attribute_filter() = ToProto(attributeFilter);

    req->Attributes().MergeFrom(Request->GetOptions());
    auto rsp = ObjectProxy->Execute(req).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

    ReplySuccess(TYsonString(rsp->value()));
}

////////////////////////////////////////////////////////////////////////////////

void TSetCommand::DoExecute()
{
    auto req = TYPathProxy::Set(Request->Path.GetPath());
    SetTransactionId(req, GetTransactionId(false));
    NMetaState::GenerateRpcMutationId(req);

    auto producer = Context->CreateInputProducer();
    TYsonString value = ConvertToYsonString(producer);
    req->set_value(value.Data());

    req->Attributes().MergeFrom(Request->GetOptions());
    auto rsp = ObjectProxy->Execute(req).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveCommand::DoExecute()
{
    auto req = TYPathProxy::Remove(Request->Path.GetPath());
    req->set_recursive(Request->Recursive);
    req->set_force(Request->Force);
    SetTransactionId(req, GetTransactionId(false));
    NMetaState::GenerateRpcMutationId(req);

    req->Attributes().MergeFrom(Request->GetOptions());
    auto rsp = ObjectProxy->Execute(req).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
}

//////////////////////////////////////////////////////////////////////////////////

void TListCommand::DoExecute()
{
    auto req = TYPathProxy::List(Request->Path.GetPath());
    SetTransactionId(req, GetTransactionId(false));

    TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly, Request->Attributes);
    *req->mutable_attribute_filter() = ToProto(attributeFilter);

    req->Attributes().MergeFrom(Request->GetOptions());
    auto rsp = ObjectProxy->Execute(req).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

    ReplySuccess(TYsonString(rsp->keys()));
}

//////////////////////////////////////////////////////////////////////////////////

void TCreateCommand::DoExecute()
{

    if (TypeIsVersioned(Request->Type)) {
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

        auto rsp = ObjectProxy->Execute(req).Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

        auto nodeId = TNodeId::FromProto(rsp->node_id());
        ReplySuccess(BuildYsonStringFluently()
            .Value(nodeId));
    } else {
        if (Request->Path) {
            THROW_ERROR_EXCEPTION("Object type is nonversioned, Cypress path is not required");
        }

        auto transactionId = GetTransactionId(false);
        auto req = TMasterYPathProxy::CreateObject();
        if (transactionId != NullTransactionId) {
            *req->mutable_transaction_id() = transactionId.ToProto();
        }
        req->set_type(Request->Type);
        NMetaState::GenerateRpcMutationId(req);

        if (Request->Attributes) {
            auto attributes = ConvertToAttributes(Request->Attributes);
            ToProto(req->mutable_object_attributes(), *attributes);
        }

        auto rsp = ObjectProxy->Execute(req).Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

        auto objectId = TObjectId::FromProto(rsp->object_id());
        ReplySuccess(BuildYsonStringFluently()
            .Value(objectId));
    }
}

//////////////////////////////////////////////////////////////////////////////////

void TLockCommand::DoExecute()
{
    auto req = TCypressYPathProxy::Lock(Request->Path.GetPath());
    SetTransactionId(req, GetTransactionId(true));
    NMetaState::GenerateRpcMutationId(req);

    req->set_mode(Request->Mode);

    req->Attributes().MergeFrom(Request->GetOptions());
    auto rsp = ObjectProxy->Execute(req).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
}

////////////////////////////////////////////////////////////////////////////////

void TCopyCommand::DoExecute()
{
    auto req = TCypressYPathProxy::Copy(Request->DestinationPath.GetPath());
    SetTransactionId(req, GetTransactionId(false));
    NMetaState::GenerateRpcMutationId(req);
    req->set_source_path(Request->SourcePath.GetPath());

    auto rsp = ObjectProxy->Execute(req).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

    auto consumer = Context->CreateOutputConsumer();
    auto nodeId = TNodeId::FromProto(rsp->object_id());
    BuildYsonFluently(~consumer)
        .Value(nodeId.ToString());
}

////////////////////////////////////////////////////////////////////////////////

void TMoveCommand::DoExecute()
{
    {
        auto req = TCypressYPathProxy::Copy(Request->DestinationPath.GetPath());
        SetTransactionId(req, GetTransactionId(false));
        NMetaState::GenerateRpcMutationId(req);
        req->set_source_path(Request->SourcePath.GetPath());
        auto rsp = ObjectProxy->Execute(req).Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
    }

    {
        auto req = TYPathProxy::Remove(Request->SourcePath.GetPath());
        req->set_recursive(true);
        SetTransactionId(req, GetTransactionId(false));
        NMetaState::GenerateRpcMutationId(req);

        auto rsp = ObjectProxy->Execute(req).Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TExistsCommand::DoExecute()
{
    auto req = TYPathProxy::Exists(Request->Path.GetPath());
    SetTransactionId(req, GetTransactionId(false));

    auto rsp = ObjectProxy->Execute(req).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

    ReplySuccess(ConvertToYsonString(rsp->value()));
}

////////////////////////////////////////////////////////////////////////////////

void TLinkCommand::DoExecute()
{
    TObjectId targetId;
    {
        auto req = TCypressYPathProxy::Get(Request->TargetPath.GetPath() + "/@id");
        SetTransactionId(req, GetTransactionId(false));

        auto rsp = ObjectProxy->Execute(req).Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
        targetId = ConvertTo<TObjectId>(TYsonString(rsp->value()));
    }

    TObjectId linkId;
    {
        auto req = TCypressYPathProxy::Create(Request->LinkPath.GetPath());
        req->set_type(EObjectType::LinkNode);
        req->set_recursive(Request->Recursive);
        req->set_ignore_existing(Request->IgnoreExisting);
        SetTransactionId(req, GetTransactionId(false));
        NMetaState::GenerateRpcMutationId(req);

        auto attributes = Request->Attributes ? ConvertToAttributes(Request->Attributes) : CreateEphemeralAttributes();
        attributes->Set("target_id", targetId);
        ToProto(req->mutable_node_attributes(), *attributes);

        auto rsp = ObjectProxy->Execute(req).Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
        linkId = TNodeId::FromProto(rsp->node_id());
    }

    ReplySuccess(BuildYsonStringFluently()
        .Value(linkId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
