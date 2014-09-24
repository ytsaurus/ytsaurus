#include "stdafx.h"
#include "cypress_commands.h"

#include <core/concurrency/scheduler.h>

#include <ytlib/object_client/helpers.h>

#include <core/ytree/fluent.h>
#include <core/ytree/attribute_helpers.h>

#include <ytlib/api/client.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NObjectClient;
using namespace NApi;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TGetCommand::DoExecute()
{
    TGetNodeOptions options;
    options.MaxSize = Request_->MaxSize;
    options.IgnoreOpaque = Request_->IgnoreOpaque;
    auto requestOptions = IAttributeDictionary::FromMap(Request_->GetOptions());
    options.Options = requestOptions.get();
    options.AttributeFilter = TAttributeFilter(EAttributeFilterMode::MatchingOnly, Request_->Attributes);
    SetTransactionalOptions(&options);
    SetSuppressableAccessTrackingOptions(&options);

    auto result = WaitFor(Context_->GetClient()->GetNode(
        Request_->Path.GetPath(),
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);

    Reply(result.Value());
}

////////////////////////////////////////////////////////////////////////////////

void TSetCommand::DoExecute()
{
    TSetNodeOptions options;
    SetTransactionalOptions(&options);
    SetMutatingOptions(&options);
    auto producer = Context_->CreateInputProducer();
    auto value = ConvertToYsonString(producer);

    auto result = WaitFor(Context_->GetClient()->SetNode(
        Request_->Path.GetPath(),
        value,
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveCommand::DoExecute()
{
    TRemoveNodeOptions options;
    options.Recursive = Request_->Recursive;
    options.Force = Request_->Force;
    SetTransactionalOptions(&options);
    SetMutatingOptions(&options);

    auto result = WaitFor(Context_->GetClient()->RemoveNode(
        Request_->Path.GetPath(),
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
}

//////////////////////////////////////////////////////////////////////////////////

void TListCommand::DoExecute()
{
    TListNodesOptions options;
    options.MaxSize = Request_->MaxSize;
    options.AttributeFilter = TAttributeFilter(EAttributeFilterMode::MatchingOnly, Request_->Attributes);
    SetTransactionalOptions(&options);
    SetSuppressableAccessTrackingOptions(&options);
    
    auto result = WaitFor(Context_->GetClient()->ListNodes(
        Request_->Path.GetPath(),
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);

    Reply(result.Value());
}

//////////////////////////////////////////////////////////////////////////////////

void TCreateCommand::DoExecute()
{
    auto attributes = Request_->Attributes
        ? ConvertToAttributes(Request_->Attributes)
        : CreateEphemeralAttributes();

    if (IsVersionedType(Request_->Type)) {
        if (!Request_->Path) {
            THROW_ERROR_EXCEPTION("Object type is versioned, Cypress path required");
        }

        TCreateNodeOptions options;
        options.Recursive = Request_->Recursive;
        options.IgnoreExisting = Request_->IgnoreExisting;
        options.Attributes = attributes.get();
        SetTransactionalOptions(&options);
        SetMutatingOptions(&options);

        auto result = WaitFor(Context_->GetClient()->CreateNode(
            Request_->Path->GetPath(),
            Request_->Type,
            options));
        THROW_ERROR_EXCEPTION_IF_FAILED(result);

        const auto& nodeId = result.Value();
        Reply(BuildYsonStringFluently().Value(nodeId));
    } else {
        if (Request_->Path) {
            THROW_ERROR_EXCEPTION("Object type is nonversioned, Cypress path is not required");
        }

        TCreateObjectOptions options;
        options.Attributes = attributes.get();
        SetTransactionalOptions(&options);
        SetMutatingOptions(&options);

        auto result = WaitFor(Context_->GetClient()->CreateObject(
            Request_->Type,
            options));
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
        
        const auto& objectId = result.Value();
        Reply(BuildYsonStringFluently().Value(objectId));
    }
}

//////////////////////////////////////////////////////////////////////////////////

void TLockCommand::DoExecute()
{
    TLockNodeOptions options;
    options.Waitable = Request_->Waitable;
    SetTransactionalOptions(&options);
    SetMutatingOptions(&options);

    auto result = WaitFor(Context_->GetClient()->LockNode(
        Request_->Path.GetPath(),
        Request_->Mode,
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);

    const auto& lockId = result.Value();
    Reply(BuildYsonStringFluently().Value(lockId));
}

////////////////////////////////////////////////////////////////////////////////

void TCopyCommand::DoExecute()
{
    TCopyNodeOptions options;
    options.PreserveAccount = Request_->PreserveAccount;
    SetTransactionalOptions(&options);
    SetMutatingOptions(&options);

    auto result = WaitFor(Context_->GetClient()->CopyNode(
        Request_->SourcePath.GetPath(),
        Request_->DestinationPath.GetPath(),
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);

    const auto& nodeId = result.Value();
    Reply(BuildYsonStringFluently().Value(nodeId));
}

////////////////////////////////////////////////////////////////////////////////

void TMoveCommand::DoExecute()
{
    TMoveNodeOptions options;
    SetTransactionalOptions(&options);
    SetMutatingOptions(&options);
    options.PreserveAccount = Request_->PreserveAccount;

    auto result = WaitFor(Context_->GetClient()->MoveNode(
        Request_->SourcePath.GetPath(),
        Request_->DestinationPath.GetPath(),
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);

    const auto& nodeId = result.Value();
    Reply(BuildYsonStringFluently().Value(nodeId));
}

////////////////////////////////////////////////////////////////////////////////

void TExistsCommand::DoExecute()
{
    TNodeExistsOptions options;
    SetTransactionalOptions(&options);

    auto result = WaitFor(Context_->GetClient()->NodeExists(
        Request_->Path.GetPath(),
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);

    Reply(BuildYsonStringFluently().Value(result.Value()));
}

////////////////////////////////////////////////////////////////////////////////

void TLinkCommand::DoExecute()
{
    TLinkNodeOptions options;
    options.Recursive = Request_->Recursive;
    options.IgnoreExisting = Request_->IgnoreExisting;
    auto attributes = Request_->Attributes
        ? ConvertToAttributes(Request_->Attributes)
        : CreateEphemeralAttributes();
    options.Attributes = attributes.get();
    SetTransactionalOptions(&options);
    SetMutatingOptions(&options);

    auto result = WaitFor(Context_->GetClient()->LinkNode(
        Request_->TargetPath.GetPath(),
        Request_->LinkPath.GetPath(),
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);

    const auto& nodeId = result.Value();
    Reply(BuildYsonStringFluently().Value(nodeId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
