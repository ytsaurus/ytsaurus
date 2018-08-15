#pragma once

#include "command.h"

#include <yt/ytlib/cypress_client/public.h>

#include <yt/client/ypath/rich.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TGetCommand
    : public TTypedCommand<NApi::TGetNodeOptions>
{
public:
    TGetCommand();

private:
    NYPath::TRichYPath Path;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TSetCommand
    : public TTypedCommand<NApi::TSetNodeOptions>
{
public:
    TSetCommand();

private:
    NYPath::TRichYPath Path;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TRemoveCommand
    : public TTypedCommand<NApi::TRemoveNodeOptions>
{
public:
    TRemoveCommand();

private:
    NYPath::TRichYPath Path;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TListCommand
    : public TTypedCommand<NApi::TListNodeOptions>
{
public:
    TListCommand();

private:
    NYPath::TRichYPath Path;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCreateCommand
    : public TCommandBase
{
public:
    TCreateCommand();

private:
    NObjectClient::EObjectType Type;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCreateNodeCommand
    : public TTypedCommand<NApi::TCreateNodeOptions>
{
public:
    TCreateNodeCommand();

private:
    NYPath::TRichYPath Path;
    NObjectClient::EObjectType Type;
    NYTree::INodePtr Attributes;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCreateObjectCommand
    : public TTypedCommand<NApi::TCreateObjectOptions>
{
public:
    TCreateObjectCommand();

private:
    NObjectClient::EObjectType Type;
    NYTree::INodePtr Attributes;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TLockCommand
    : public TTypedCommand<NApi::TLockNodeOptions>
{
public:
    TLockCommand();

private:
    NYPath::TRichYPath Path;
    NCypressClient::ELockMode Mode;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCopyCommand
    : public TTypedCommand<NApi::TCopyNodeOptions>
{
public:
    TCopyCommand();

private:
    NYPath::TRichYPath SourcePath;
    NYPath::TRichYPath DestinationPath;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TMoveCommand
    : public TTypedCommand<NApi::TMoveNodeOptions>
{
public:
    TMoveCommand();

private:
    NYPath::TRichYPath SourcePath;
    NYPath::TRichYPath DestinationPath;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TExistsCommand
    : public TTypedCommand<NApi::TNodeExistsOptions>
{
public:
    TExistsCommand();

private:
    NYPath::TRichYPath Path;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TLinkCommand
    : public TTypedCommand<NApi::TLinkNodeOptions>
{
public:
    TLinkCommand();

private:
    NYPath::TRichYPath LinkPath;
    NYPath::TRichYPath TargetPath;
    NYTree::INodePtr Attributes;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TConcatenateCommand
    : public TTypedCommand<NApi::TConcatenateNodesOptions>
{
public:
    TConcatenateCommand();

private:
    std::vector<NYPath::TRichYPath> SourcePaths;
    NYPath::TRichYPath DestinationPath;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

