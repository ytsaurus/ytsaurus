#include "cypress_commands.h"

#include <yt/ytlib/api/client.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/ytree/attribute_helpers.h>
#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NObjectClient;
using namespace NApi;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TGetCommand::Execute(ICommandContextPtr context)
{
    Options.AttributeFilter = TAttributeFilter(EAttributeFilterMode::MatchingOnly, Attributes);
    Options.Options = IAttributeDictionary::FromMap(GetOptions());

    auto asyncResult = context->GetClient()->GetNode(
        Path.GetPath(),
        Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    context->ProduceOutputValue(result);
}

////////////////////////////////////////////////////////////////////////////////

void TSetCommand::Execute(ICommandContextPtr context)
{
    auto value = context->ConsumeInputValue();

    auto asyncResult = context->GetClient()->SetNode(
        Path.GetPath(),
        value,
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveCommand::Execute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->RemoveNode(
        Path.GetPath(),
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();
}

//////////////////////////////////////////////////////////////////////////////////

void TListCommand::Execute(ICommandContextPtr context)
{
    Options.AttributeFilter = TAttributeFilter(EAttributeFilterMode::MatchingOnly, Attributes);

    auto asyncResult = context->GetClient()->ListNode(
        Path.GetPath(),
        Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    context->ProduceOutputValue(result);
}

//////////////////////////////////////////////////////////////////////////////////

void TCreateCommand::Execute(ICommandContextPtr context)
{
    Options.Attributes = Attributes
        ? ConvertToAttributes(Attributes)
        : CreateEphemeralAttributes();

    if (IsVersionedType(Type)) {
        if (!Path) {
            THROW_ERROR_EXCEPTION("Object type is versioned, Cypress path required");
        }

        TCreateNodeOptions options;
        static_cast<TCreateObjectOptions&>(options) = Options;
        options.Recursive = Recursive;
        options.IgnoreExisting = IgnoreExisting;

        auto asyncNodeId = context->GetClient()->CreateNode(
            Path->GetPath(),
            Type,
            options);
        auto nodeId = WaitFor(asyncNodeId)
            .ValueOrThrow();

        context->ProduceOutputValue(BuildYsonStringFluently()
            .Value(nodeId));
    } else {
        if (Path) {
            THROW_ERROR_EXCEPTION("Object type is nonversioned, Cypress path is not required");
        }

        auto asyncObjectId = context->GetClient()->CreateObject(
            Type,
            Options);
        auto objectId = WaitFor(asyncObjectId)
            .ValueOrThrow();

        context->ProduceOutputValue(BuildYsonStringFluently()
            .Value(objectId));
    }
}

//////////////////////////////////////////////////////////////////////////////////

void TLockCommand::Execute(ICommandContextPtr context)
{
    auto asyncLockId = context->GetClient()->LockNode(
        Path.GetPath(),
        Mode,
        Options);
    auto lockId = WaitFor(asyncLockId)
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .Value(lockId));
}

////////////////////////////////////////////////////////////////////////////////

void TCopyCommand::Execute(ICommandContextPtr context)
{
    auto asyncNodeId = context->GetClient()->CopyNode(
        SourcePath.GetPath(),
        DestinationPath.GetPath(),
        Options);
    auto nodeId = WaitFor(asyncNodeId)
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .Value(nodeId));
}

////////////////////////////////////////////////////////////////////////////////

void TMoveCommand::Execute(ICommandContextPtr context)
{
    auto asyncNodeId = context->GetClient()->MoveNode(
        SourcePath.GetPath(),
        DestinationPath.GetPath(),
        Options);
    auto nodeId = WaitFor(asyncNodeId)
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .Value(nodeId));
}

////////////////////////////////////////////////////////////////////////////////

void TExistsCommand::Execute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->NodeExists(
        Path.GetPath(),
        Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .Value(result));
}

////////////////////////////////////////////////////////////////////////////////

void TLinkCommand::Execute(ICommandContextPtr context)
{
    Options.Attributes = Attributes
        ? ConvertToAttributes(Attributes)
        : CreateEphemeralAttributes();

    auto asyncNodeId = context->GetClient()->LinkNode(
        TargetPath.GetPath(),
        LinkPath.GetPath(),
        Options);
    auto nodeId = WaitFor(asyncNodeId)
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .Value(nodeId));
}

////////////////////////////////////////////////////////////////////////////////

void TConcatenateCommand::Execute(ICommandContextPtr context)
{
    std::vector<TYPath> sourcePaths;
    for (const auto& path : SourcePaths) {
        sourcePaths.push_back(path.GetPath());
    }

    Options.Append = DestinationPath.GetAppend();
    auto destinationPath = DestinationPath.GetPath();

    auto asyncResult = context->GetClient()->ConcatenateNodes(
        sourcePaths,
        DestinationPath.GetPath(),
        Options);

    WaitFor(asyncResult)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
