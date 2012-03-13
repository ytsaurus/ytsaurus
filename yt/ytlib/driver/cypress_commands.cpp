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

void TGetCommand::DoExecute(const yvector<Stroka>& args)
{
    TCypressServiceProxy proxy(DriverImpl->GetMasterChannel());

    auto ypathRequest = TYPathProxy::Get(WithTransaction(
        PathArg->getValue(),
        TxArg->getValue()));

    ypathRequest->Attributes().MergeFrom(*~GetOpts());
    auto ypathResponse = proxy.Execute(~ypathRequest)->Get();

    if (ypathResponse->IsOK()) {
        TYson value = ypathResponse->value();
        DriverImpl->ReplySuccess(value);
    } else {
        DriverImpl->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TSetCommand::DoExecute(const yvector<Stroka>& args)
{
    TCypressServiceProxy proxy(DriverImpl->GetMasterChannel());

    auto ypathRequest = TYPathProxy::Set(WithTransaction(
        PathArg->getValue(),
        TxArg->getValue()));

    ypathRequest->Attributes().MergeFrom(*~GetOpts());

    TYson value = ValueArg->getValue();
    ValidateYson(value);
    ypathRequest->set_value(value);

    auto ypathResponse = proxy.Execute(~ypathRequest)->Get();

    if (ypathResponse->IsOK()) {
        DriverImpl->ReplySuccess();
    } else {
        DriverImpl->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveCommand::DoExecute(const yvector<Stroka>& args)
{
    TCypressServiceProxy proxy(DriverImpl->GetMasterChannel());
    auto ypathRequest = TYPathProxy::Remove(WithTransaction(
        PathArg->getValue(),
        TxArg->getValue()));

    ypathRequest->Attributes().MergeFrom(*~GetOpts());

    auto ypathResponse = proxy.Execute(~ypathRequest)->Get();

    if (ypathResponse->IsOK()) {
        DriverImpl->ReplySuccess();
    } else {
        DriverImpl->ReplyError(ypathResponse->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TListCommand::DoExecute(const yvector<Stroka>& args)
{
    TCypressServiceProxy proxy(DriverImpl->GetMasterChannel());
    auto ypathRequest = TYPathProxy::List(WithTransaction(
        PathArg->getValue(),
        TxArg->getValue()));

    ypathRequest->Attributes().MergeFrom(*~GetOpts());

    auto ypathResponse = proxy.Execute(~ypathRequest)->Get();

    if (ypathResponse->IsOK()) {
         auto consumer = DriverImpl->CreateOutputConsumer();
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

void TCreateCommand::DoExecute(const yvector<Stroka>& args)
{
    TCypressServiceProxy proxy(DriverImpl->GetMasterChannel());
    auto ypathRequest = TCypressYPathProxy::Create(WithTransaction(
        PathArg->getValue(),
        TxArg->getValue()));

    ypathRequest->set_type(TypeArg->getValue());

    auto manifest = ManifestArg->getValue();
    if (!manifest.empty()) {
        ValidateYson(manifest);
        ypathRequest->set_manifest(manifest);
    }

    ypathRequest->Attributes().MergeFrom(*~GetOpts());
    auto ypathResponse = proxy.Execute(~ypathRequest)->Get();

    if (ypathResponse->IsOK()) {
        auto consumer = DriverImpl->CreateOutputConsumer();
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

void TLockCommand::DoExecute(const yvector<Stroka>& args)
{
    TCypressServiceProxy proxy(DriverImpl->GetMasterChannel());
    auto ypathRequest = TCypressYPathProxy::Lock(WithTransaction(
        PathArg->getValue(),
        TxArg->getValue()));

    auto ypathResponse = proxy.Execute(~ypathRequest)->Get();
    ypathRequest->Attributes().MergeFrom(*~GetOpts());

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
