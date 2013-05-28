#include "stdafx.h"
#include "cypress_commands.h"

#include <ytlib/actions/async_pipeline.h>
#include <ytlib/actions/invoker_util.h>

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

namespace {

template<class TResponse>
TAsyncError ExtractError(TFuture<TResponse> rspFuture)
{
    return rspFuture.Apply(BIND([] (TResponse rsp) {
        return MakeFuture(TError(*rsp));
    }));
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

void TGetCommand::DoExecute()
{
    auto req = TYPathProxy::Get(Request->Path.GetPath());
    SetTransactionId(req, EAllowNullTransaction::Yes);

    TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly, Request->Attributes);
    ToProto(req->mutable_attribute_filter(), attributeFilter);
    if (Request->MaxSize) {
        req->set_max_size(*Request->MaxSize);
    }

    CheckAndReply(
        ObjectProxy->Execute(req),
        BIND([] (TYPathProxy::TRspGetPtr rsp) {
            return TYsonString(rsp->value());
        }));
}

////////////////////////////////////////////////////////////////////////////////

void TSetCommand::DoExecute()
{
    auto req = TYPathProxy::Set(Request->Path.GetPath());
    SetTransactionId(req, EAllowNullTransaction::Yes);
    GenerateMutationId(req);
    auto producer = Context->CreateInputProducer();
    auto value = ConvertToYsonString(producer);
    req->set_value(value.Data());

    CheckAndReply(ObjectProxy->Execute(req));
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveCommand::DoExecute()
{
    auto req = TYPathProxy::Remove(Request->Path.GetPath());
    SetTransactionId(req, EAllowNullTransaction::Yes);
    GenerateMutationId(req);
    req->set_recursive(Request->Recursive);
    req->set_force(Request->Force);
    req->MutableAttributes()->MergeFrom(Request->GetOptions());

    CheckAndReply(ObjectProxy->Execute(req));
}

//////////////////////////////////////////////////////////////////////////////////

void TListCommand::DoExecute()
{
    auto req = TYPathProxy::List(Request->Path.GetPath());
    SetTransactionId(req, EAllowNullTransaction::Yes);

    TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly, Request->Attributes);
    ToProto(req->mutable_attribute_filter(), attributeFilter);
    if (Request->MaxSize) {
        req->set_max_size(*Request->MaxSize);
    }
    
    CheckAndReply(
        ObjectProxy->Execute(req),
        BIND([] (TYPathProxy::TRspListPtr rsp) {
            return TYsonString(rsp->keys());
        }));
}

//////////////////////////////////////////////////////////////////////////////////

void TCreateCommand::DoExecute()
{

    if (TypeIsVersioned(Request->Type)) {
        if (!Request->Path) {
            THROW_ERROR_EXCEPTION("Object type is versioned, Cypress path required");
        }

        auto req = TCypressYPathProxy::Create(Request->Path.Get().GetPath());
        SetTransactionId(req, EAllowNullTransaction::Yes);
        GenerateMutationId(req);
        req->set_type(Request->Type);
        req->set_recursive(Request->Recursive);
        req->set_ignore_existing(Request->IgnoreExisting);

        if (Request->Attributes) {
            auto attributes = ConvertToAttributes(Request->Attributes);
            ToProto(req->mutable_node_attributes(), *attributes);
        }

        CheckAndReply(
            ObjectProxy->Execute(req),
            BIND([] (TCypressYPathProxy::TRspCreatePtr rsp) {
                auto nodeId = FromProto<TNodeId>(rsp->node_id());
                return BuildYsonStringFluently().Value(nodeId);
            }));
    } else {
        if (Request->Path) {
            THROW_ERROR_EXCEPTION("Object type is nonversioned, Cypress path is not required");
        }

        auto transactionId = GetTransactionId(EAllowNullTransaction::Yes);
        auto req = TMasterYPathProxy::CreateObject();
        GenerateMutationId(req);
        if (transactionId != NullTransactionId) {
            ToProto(req->mutable_transaction_id(), transactionId);
        }
        req->set_type(Request->Type);
        if (Request->Attributes) {
            auto attributes = ConvertToAttributes(Request->Attributes);
            ToProto(req->mutable_object_attributes(), *attributes);
        }

        CheckAndReply(
            ObjectProxy->Execute(req),
            BIND([] (TMasterYPathProxy::TRspCreateObjectPtr rsp) {
                auto objectId = FromProto<TNodeId>(rsp->object_id());
                return BuildYsonStringFluently().Value(objectId);
            }));
    }
}

//////////////////////////////////////////////////////////////////////////////////

void TLockCommand::DoExecute()
{
    auto req = TCypressYPathProxy::Lock(Request->Path.GetPath());
    SetTransactionId(req, EAllowNullTransaction::No);
    
    GenerateMutationId(req);
    req->set_mode(Request->Mode);

    CheckAndReply(ObjectProxy->Execute(req));
}

////////////////////////////////////////////////////////////////////////////////

void TCopyCommand::DoExecute()
{
    auto req = TCypressYPathProxy::Copy(Request->DestinationPath.GetPath());
    SetTransactionId(req, EAllowNullTransaction::Yes);
    GenerateMutationId(req);
    req->set_source_path(Request->SourcePath.GetPath());

    CheckAndReply(
        ObjectProxy->Execute(req),
        BIND([] (TCypressYPathProxy::TRspCopyPtr rsp) {
            auto objectId = FromProto<TNodeId>(rsp->object_id());
            return BuildYsonStringFluently().Value(objectId);
        }));
}

////////////////////////////////////////////////////////////////////////////////

void TMoveCommand::DoExecute()
{
    auto this_ = MakeStrong(this);
    StartAsyncPipeline(GetSyncInvoker())
        ->Add(BIND([this, this_] () {
            auto copyReq = TCypressYPathProxy::Copy(Request->DestinationPath.GetPath());
            SetTransactionId(copyReq, EAllowNullTransaction::Yes);
            GenerateMutationId(copyReq);
            copyReq->set_source_path(Request->SourcePath.GetPath());
            return ExtractError(ObjectProxy->Execute(copyReq));
        }))
        ->Add(BIND([this, this_] () {
            auto removeReq = TYPathProxy::Remove(Request->SourcePath.GetPath());
            removeReq->set_recursive(true);
            SetTransactionId(removeReq, EAllowNullTransaction::Yes);
            GenerateMutationId(removeReq);
            return ExtractError(ObjectProxy->Execute(removeReq));
        }))
        ->Run().Apply(BIND([this, this_] (TValueOrError<void> error) {
            if (error.IsOK()) {
                ReplySuccess();
            }
            else {
                ReplyError(error);
            }
        }));
}

////////////////////////////////////////////////////////////////////////////////

void TExistsCommand::DoExecute()
{
    auto req = TYPathProxy::Exists(Request->Path.GetPath());
    SetTransactionId(req, EAllowNullTransaction::Yes);

    CheckAndReply(
        ObjectProxy->Execute(req),
        BIND([] (TYPathProxy::TRspExistsPtr rsp) {
            return ConvertToYsonString(rsp->value());
        }));
}

////////////////////////////////////////////////////////////////////////////////

void TLinkCommand::DoExecute()
{
    auto this_ = MakeStrong(this);
    StartAsyncPipeline(GetSyncInvoker())
        ->Add(BIND([this, this_] () {
            auto req = TCypressYPathProxy::Get(Request->TargetPath.GetPath() + "/@id");
            SetTransactionId(req, EAllowNullTransaction::Yes);

            return ObjectProxy->Execute(req);
        }))
        ->Add(BIND([this, this_] (TCypressYPathProxy::TRspGetPtr rsp) {
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
            auto targetId = ConvertTo<TObjectId>(TYsonString(rsp->value()));

            auto req = TCypressYPathProxy::Create(Request->LinkPath.GetPath());
            req->set_type(EObjectType::LinkNode);
            req->set_recursive(Request->Recursive);
            req->set_ignore_existing(Request->IgnoreExisting);
            SetTransactionId(req, EAllowNullTransaction::Yes);
            GenerateMutationId(req);

            auto attributes = Request->Attributes ? ConvertToAttributes(Request->Attributes) : CreateEphemeralAttributes();
            attributes->Set("target_id", targetId);
            ToProto(req->mutable_node_attributes(), *attributes);

            return ObjectProxy->Execute(req);
        }))
        ->Run().Apply(BIND([this, this_] (TValueOrError<TCypressYPathProxy::TRspCreatePtr> rspOrError) {
            if (!rspOrError.IsOK()) {
                ReplyError(rspOrError);
            }
            else {
                OnProxyResponse(
                    BIND([] (TCypressYPathProxy::TRspCreatePtr rsp) {
                        auto linkId = FromProto<TNodeId>(rsp->node_id());
                        return BuildYsonStringFluently().Value(linkId);
                    }),
                    rspOrError.Value()
                );
            }
        }));

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
