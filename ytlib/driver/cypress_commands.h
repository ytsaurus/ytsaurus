#pragma once

#include "command.h"

#include <yt/ytlib/cypress_client/public.h>

#include <yt/ytlib/ypath/rich.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TGetCommand
    : public TTypedCommand<NApi::TGetNodeOptions>
{
private:
    NYPath::TRichYPath Path;

public:
    TGetCommand()
    {
        RegisterParameter("path", Path);
        RegisterParameter("attributes", Options.Attributes)
            .Optional();
        // TODO(babenko): rename to "limit"
        RegisterParameter("max_size", Options.MaxSize)
            .Optional();
        RegisterParameter("ignore_opaque", Options.IgnoreOpaque)
            .Optional();
    }

    void Execute(ICommandContextPtr context);

};

class TSetCommand
    : public TTypedCommand<NApi::TSetNodeOptions>
{
private:
    NYPath::TRichYPath Path;

public:
    TSetCommand()
    {
        RegisterParameter("path", Path);
    }

    void Execute(ICommandContextPtr context);

};

class TRemoveCommand
    : public TTypedCommand<NApi::TRemoveNodeOptions>
{
private:
    NYPath::TRichYPath Path;

public:
    TRemoveCommand()
    {
        RegisterParameter("path", Path);
        RegisterParameter("recursive", Options.Recursive)
            .Optional();
        RegisterParameter("force", Options.Force)
            .Optional();
    }

    void Execute(ICommandContextPtr context);

};

class TListCommand
    : public TTypedCommand<NApi::TListNodeOptions>
{
private:
    NYPath::TRichYPath Path;

public:
    TListCommand()
    {
        RegisterParameter("path", Path);
        RegisterParameter("attributes", Options.Attributes)
            .Optional();
        // TODO(babenko): rename to "limit"
        RegisterParameter("max_size", Options.MaxSize)
            .Optional();
    }

    void Execute(ICommandContextPtr context);

};

class TCreateCommand
    : public TTypedCommand<NApi::TCreateObjectOptions>
{
private:
    TNullable<NYPath::TRichYPath> Path;
    NObjectClient::EObjectType Type;
    NYTree::INodePtr Attributes;
    bool Recursive;
    bool IgnoreExisting;
    NTransactionClient::TTransactionId TransactionId;

public:
    TCreateCommand()
    {
        RegisterParameter("path", Path)
            .Optional();
        RegisterParameter("type", Type);
        RegisterParameter("attributes", Attributes)
            .Optional();
        RegisterParameter("recursive", Recursive)
            .Default(false);
        RegisterParameter("ignore_existing", IgnoreExisting)
            .Default(false);
        RegisterParameter("transaction_id", TransactionId)
            .Default(NTransactionClient::NullTransactionId);
    }

    void Execute(ICommandContextPtr context);

};

class TLockCommand
    : public TTypedCommand<NApi::TLockNodeOptions>
{
private:
    NYPath::TRichYPath Path;
    NCypressClient::ELockMode Mode;

public:
    TLockCommand()
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

        RegisterValidator([&] () {
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

    void Execute(ICommandContextPtr context);

};

class TCopyCommand
    : public TTypedCommand<NApi::TCopyNodeOptions>
{
private:
    NYPath::TRichYPath SourcePath;
    NYPath::TRichYPath DestinationPath;

public:
    TCopyCommand()
    {
        RegisterParameter("source_path", SourcePath);
        RegisterParameter("destination_path", DestinationPath);
        RegisterParameter("recursive", Options.Recursive)
            .Optional();
        RegisterParameter("force", Options.Force)
            .Optional();
        RegisterParameter("preserve_account", Options.PreserveAccount)
            .Optional();
    }

    void Execute(ICommandContextPtr context);

};

class TMoveCommand
    : public TTypedCommand<NApi::TMoveNodeOptions>
{
private:
    NYPath::TRichYPath SourcePath;
    NYPath::TRichYPath DestinationPath;

public:
    TMoveCommand()
    {
        RegisterParameter("source_path", SourcePath);
        RegisterParameter("destination_path", DestinationPath);
        RegisterParameter("recursive", Options.Recursive)
            .Optional();
        RegisterParameter("force", Options.Force)
            .Optional();
        RegisterParameter("preserve_account", Options.PreserveAccount)
            .Optional();
    }

    void Execute(ICommandContextPtr context);

};

class TExistsCommand
    : public TTypedCommand<NApi::TNodeExistsOptions>
{
private:
    NYPath::TRichYPath Path;

public:
    TExistsCommand()
    {
        RegisterParameter("path", Path);
    }

    void Execute(ICommandContextPtr context);

};

class TLinkCommand
    : public TTypedCommand<NApi::TLinkNodeOptions>
{
private:
    NYPath::TRichYPath LinkPath;
    NYPath::TRichYPath TargetPath;
    NYTree::INodePtr Attributes;

public:
    TLinkCommand()
    {
        RegisterParameter("link_path", LinkPath);
        RegisterParameter("target_path", TargetPath);
        RegisterParameter("attributes", Attributes)
            .Optional();
        RegisterParameter("recursive", Options.Recursive)
            .Optional();
        RegisterParameter("ignore_existing", Options.IgnoreExisting)
            .Optional();
    }

    void Execute(ICommandContextPtr context);

};

////////////////////////////////////////////////////////////////////////////////

class TConcatenateCommand
    : public TTypedCommand<NApi::TConcatenateNodesOptions>
{
private:
    std::vector<NYPath::TRichYPath> SourcePaths;
    NYPath::TRichYPath DestinationPath;

public:
    TConcatenateCommand()
    {
        RegisterParameter("source_paths", SourcePaths);
        RegisterParameter("destination_path", DestinationPath);
    }

    virtual void OnLoaded() override
    {
        TCommandBase::OnLoaded();

        for (auto& path : SourcePaths) {
            path = path.Normalize();
        }
        DestinationPath = DestinationPath.Normalize();
    }

    void Execute(ICommandContextPtr context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

