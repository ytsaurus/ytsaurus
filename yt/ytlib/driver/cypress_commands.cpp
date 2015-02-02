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
    options.Options = IAttributeDictionary::FromMap(Request_->GetOptions());
    options.AttributeFilter = TAttributeFilter(
        EAttributeFilterMode::MatchingOnly,
        Request_->Attributes);
    SetTransactionalOptions(&options);
    SetReadOnlyOptions(&options);
    SetSuppressableAccessTrackingOptions(&options);

    auto asyncResult = Context_->GetClient()->GetNode(
        Request_->Path.GetPath(),
        options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    Reply(result);
}

////////////////////////////////////////////////////////////////////////////////

void TSetCommand::DoExecute()
{
    TSetNodeOptions options;
    SetTransactionalOptions(&options);
    SetMutatingOptions(&options);
    auto producer = Context_->CreateInputProducer();
    auto value = ConvertToYsonString(producer);

    auto asyncResult = Context_->GetClient()->SetNode(
        Request_->Path.GetPath(),
        value,
        options);
    WaitFor(asyncResult)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveCommand::DoExecute()
{
    TRemoveNodeOptions options;
    options.Recursive = Request_->Recursive;
    options.Force = Request_->Force;
    SetTransactionalOptions(&options);
    SetMutatingOptions(&options);

    auto asyncResult = Context_->GetClient()->RemoveNode(
        Request_->Path.GetPath(),
        options);
    WaitFor(asyncResult)
        .ThrowOnError();
}

//////////////////////////////////////////////////////////////////////////////////

void TListCommand::DoExecute()
{
    TListNodeOptions options;
    options.MaxSize = Request_->MaxSize;
    options.AttributeFilter = TAttributeFilter(EAttributeFilterMode::MatchingOnly, Request_->Attributes);
    SetTransactionalOptions(&options);
    SetReadOnlyOptions(&options);
    SetSuppressableAccessTrackingOptions(&options);
    
    auto asyncResult = Context_->GetClient()->ListNode(
        Request_->Path.GetPath(),
        options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    Reply(result);
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
        options.Attributes = std::move(attributes);
        SetTransactionalOptions(&options);
        SetMutatingOptions(&options);

        auto asyncNodeId = Context_->GetClient()->CreateNode(
            Request_->Path->GetPath(),
            Request_->Type,
            options);
        auto nodeId = WaitFor(asyncNodeId)
            .ValueOrThrow();

        Reply(BuildYsonStringFluently()
            .Value(nodeId));
    } else {
        if (Request_->Path) {
            THROW_ERROR_EXCEPTION("Object type is nonversioned, Cypress path is not required");
        }

        TCreateObjectOptions options;
        options.Attributes = std::move(attributes);
        SetTransactionalOptions(&options);
        SetMutatingOptions(&options);

        auto asyncObjectId = Context_->GetClient()->CreateObject(
            Request_->Type,
            options);
        auto objectId = WaitFor(asyncObjectId)
            .ValueOrThrow();

        Reply(BuildYsonStringFluently()
            .Value(objectId));
    }
}

//////////////////////////////////////////////////////////////////////////////////

void TLockCommand::DoExecute()
{
    TLockNodeOptions options;
    options.Waitable = Request_->Waitable;
    SetTransactionalOptions(&options);
    SetMutatingOptions(&options);

    auto asyncLockId = Context_->GetClient()->LockNode(
        Request_->Path.GetPath(),
        Request_->Mode,
        options);
    auto lockId = WaitFor(asyncLockId)
        .ValueOrThrow();

    Reply(BuildYsonStringFluently()
        .Value(lockId));
}

////////////////////////////////////////////////////////////////////////////////

void TCopyCommand::DoExecute()
{
    TCopyNodeOptions options;
    options.Recursive = Request_->Recursive;
    options.PreserveAccount = Request_->PreserveAccount;
    SetTransactionalOptions(&options);
    SetMutatingOptions(&options);

    auto asyncNodeId = Context_->GetClient()->CopyNode(
        Request_->SourcePath.GetPath(),
        Request_->DestinationPath.GetPath(),
        options);
    auto nodeId = WaitFor(asyncNodeId)
        .ValueOrThrow();

    Reply(BuildYsonStringFluently()
        .Value(nodeId));
}

////////////////////////////////////////////////////////////////////////////////

void TMoveCommand::DoExecute()
{
    TMoveNodeOptions options;
    SetTransactionalOptions(&options);
    SetMutatingOptions(&options);
    options.Recursive = Request_->Recursive;
    options.PreserveAccount = Request_->PreserveAccount;

    auto asyncNodeId = Context_->GetClient()->MoveNode(
        Request_->SourcePath.GetPath(),
        Request_->DestinationPath.GetPath(),
        options);
    auto nodeId = WaitFor(asyncNodeId)
        .ValueOrThrow();

    Reply(BuildYsonStringFluently()
        .Value(nodeId));
}

////////////////////////////////////////////////////////////////////////////////

void TExistsCommand::DoExecute()
{
    TNodeExistsOptions options;
    SetTransactionalOptions(&options);
    SetReadOnlyOptions(&options);

    auto asyncResult = Context_->GetClient()->NodeExists(
        Request_->Path.GetPath(),
        options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    Reply(BuildYsonStringFluently()
        .Value(result));
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
    options.Attributes = std::move(attributes);
    SetTransactionalOptions(&options);
    SetMutatingOptions(&options);

    auto asyncNodeId = Context_->GetClient()->LinkNode(
        Request_->TargetPath.GetPath(),
        Request_->LinkPath.GetPath(),
        options);
    auto nodeId = WaitFor(asyncNodeId)
        .ValueOrThrow();

    Reply(BuildYsonStringFluently()
        .Value(nodeId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
