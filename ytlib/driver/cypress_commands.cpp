#include "cypress_commands.h"

#include <yt/client/api/client.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/ytree/helpers.h>
#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NObjectClient;
using namespace NApi;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TGetCommand::TGetCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("attributes", Options.Attributes)
        .Optional();
    // TODO(babenko): rename to "limit"
    RegisterParameter("max_size", Options.MaxSize)
        .Optional();
}

void TGetCommand::DoExecute(ICommandContextPtr context)
{
    Options.Options = IAttributeDictionary::FromMap(GetUnrecognized());

    auto asyncResult = context->GetClient()->GetNode(
        RewritePath(Path.GetPath(), RewriteOperationPath),
        Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "value", result);
}

////////////////////////////////////////////////////////////////////////////////

TSetCommand::TSetCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("recursive", Options.Recursive)
        .Optional();
}

void TSetCommand::DoExecute(ICommandContextPtr context)
{
    auto value = context->ConsumeInputValue();

    auto asyncResult = context->GetClient()->SetNode(
        RewritePath(Path.GetPath(), RewriteOperationPath),
        value,
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TRemoveCommand::TRemoveCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("recursive", Options.Recursive)
        .Optional();
    RegisterParameter("force", Options.Force)
        .Optional();
}

void TRemoveCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->RemoveNode(
        Path.GetPath(),
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TListCommand::TListCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("attributes", Options.Attributes)
        .Default(std::vector<TString>());
    // TODO(babenko): rename to "limit"
    RegisterParameter("max_size", Options.MaxSize)
        .Optional();
}

void TListCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->ListNode(
        RewritePath(Path.GetPath(), RewriteOperationPath),
        Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "value", result);
}

////////////////////////////////////////////////////////////////////////////////

TCreateCommand::TCreateCommand()
{
    RegisterParameter("type", Type);
}

void TCreateCommand::DoExecute(ICommandContextPtr context)
{
    // For historical reasons, we handle both CreateNode and CreateObject requests
    // in a single command. Here we route the request to an appropriate backend command.
    auto backend = IsVersionedType(Type)
        ? std::unique_ptr<ICommand>(new TCreateNodeCommand())
        : std::unique_ptr<ICommand>(new TCreateObjectCommand());
    backend->Execute(context);
}

////////////////////////////////////////////////////////////////////////////////

TCreateNodeCommand::TCreateNodeCommand()
{
    RegisterParameter("path", Path)
        .Optional();
    RegisterParameter("type", Type);
    RegisterParameter("attributes", Attributes)
        .Optional();
    RegisterParameter("recursive", Options.Recursive)
        .Optional();
    RegisterParameter("ignore_existing", Options.IgnoreExisting)
        .Optional();
    RegisterParameter("force", Options.Force)
        .Optional();
}

void TCreateNodeCommand::DoExecute(ICommandContextPtr context)
{
    Options.Attributes = Attributes
        ? ConvertToAttributes(Attributes)
        : CreateEphemeralAttributes();

    auto asyncNodeId = context->GetClient()->CreateNode(
        RewritePath(Path.GetPath(), RewriteOperationPath),
        Type,
        Options);
    auto nodeId = WaitFor(asyncNodeId)
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "node_id", nodeId);
}

////////////////////////////////////////////////////////////////////////////////

TCreateObjectCommand::TCreateObjectCommand()
{
    RegisterParameter("type", Type);
    RegisterParameter("attributes", Attributes)
        .Optional();
}

void TCreateObjectCommand::DoExecute(ICommandContextPtr context)
{
    Options.Attributes = Attributes
        ? ConvertToAttributes(Attributes)
        : CreateEphemeralAttributes();

    auto asyncObjectId = context->GetClient()->CreateObject(
        Type,
        Options);
    auto objectId = WaitFor(asyncObjectId)
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "object_id", objectId);
}

////////////////////////////////////////////////////////////////////////////////

TLockCommand::TLockCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("mode", Mode)
        .Default(NCypressClient::ELockMode::Exclusive);
    RegisterParameter("waitable", Options.Waitable)
        .Optional();
    RegisterParameter("child_key", Options.ChildKey)
        .Optional();
    RegisterParameter("attribute_key", Options.AttributeKey)
        .Optional();

    RegisterPostprocessor([&] () {
        if (Mode != NCypressClient::ELockMode::Shared) {
            if (Options.ChildKey) {
                THROW_ERROR_EXCEPTION("\"child_key\" can only be specified for shared locks");
            }
            if (Options.AttributeKey) {
                THROW_ERROR_EXCEPTION("\"attribute_key\" can only be specified for shared locks");
            }
        }
        if (Options.ChildKey && Options.AttributeKey) {
            THROW_ERROR_EXCEPTION("Cannot specify both \"child_key\" and \"attribute_key\"");
        }
    });
}

void TLockCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncLockResult = context->GetClient()->LockNode(
        RewritePath(Path.GetPath(), RewriteOperationPath),
        Mode,
        Options);
    auto lockResult = WaitFor(asyncLockResult)
        .ValueOrThrow();

    ProduceOutput(context,
        [&](NYson::IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .Value(lockResult.LockId);
        },
        [&](NYson::IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("lock_id").Value(lockResult.LockId)
                    .Item("node_id").Value(lockResult.NodeId)
                .EndMap();
        });
}

////////////////////////////////////////////////////////////////////////////////

TCopyCommand::TCopyCommand()
{
    RegisterParameter("source_path", SourcePath);
    RegisterParameter("destination_path", DestinationPath);
    RegisterParameter("recursive", Options.Recursive)
        .Optional();
    RegisterParameter("force", Options.Force)
        .Optional();
    RegisterParameter("preserve_account", Options.PreserveAccount)
        .Optional();
    RegisterParameter("preserve_expiration_time", Options.PreserveExpirationTime)
        .Optional();
    RegisterParameter("preserve_creation_time", Options.PreserveCreationTime)
        .Optional();
}

void TCopyCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncNodeId = context->GetClient()->CopyNode(
        SourcePath.GetPath(),
        DestinationPath.GetPath(),
        Options);
    auto nodeId = WaitFor(asyncNodeId)
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "node_id", nodeId);
}

////////////////////////////////////////////////////////////////////////////////

TMoveCommand::TMoveCommand()
{
    RegisterParameter("source_path", SourcePath);
    RegisterParameter("destination_path", DestinationPath);
    RegisterParameter("recursive", Options.Recursive)
        .Optional();
    RegisterParameter("force", Options.Force)
        .Optional();
    RegisterParameter("preserve_account", Options.PreserveAccount)
        .Optional();
    RegisterParameter("preserve_expiration_time", Options.PreserveExpirationTime)
        .Optional();
}

void TMoveCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncNodeId = context->GetClient()->MoveNode(
        SourcePath.GetPath(),
        DestinationPath.GetPath(),
        Options);
    auto nodeId = WaitFor(asyncNodeId)
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "node_id", nodeId);
}

////////////////////////////////////////////////////////////////////////////////

TExistsCommand::TExistsCommand()
{
    RegisterParameter("path", Path);
}

void TExistsCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->NodeExists(
        RewritePath(Path.GetPath(), RewriteOperationPath),
        Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "value", result);
}

////////////////////////////////////////////////////////////////////////////////

TLinkCommand::TLinkCommand()
{
    RegisterParameter("link_path", LinkPath);
    RegisterParameter("target_path", TargetPath);
    RegisterParameter("attributes", Attributes)
        .Optional();
    RegisterParameter("recursive", Options.Recursive)
        .Optional();
    RegisterParameter("ignore_existing", Options.IgnoreExisting)
        .Optional();
    RegisterParameter("force", Options.Force)
        .Optional();
}

void TLinkCommand::DoExecute(ICommandContextPtr context)
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

    ProduceSingleOutputValue(context, "node_id", nodeId);
}

////////////////////////////////////////////////////////////////////////////////

TConcatenateCommand::TConcatenateCommand()
{
    RegisterParameter("source_paths", SourcePaths);
    RegisterParameter("destination_path", DestinationPath);

    RegisterPostprocessor([&] {
        for (auto& path : SourcePaths) {
            path = path.Normalize();
        }
        DestinationPath = DestinationPath.Normalize();
    });
}

void TConcatenateCommand::DoExecute(ICommandContextPtr context)
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

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
