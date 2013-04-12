#pragma once

#include "command.h"

#include <ytlib/ypath/rich.h>

#include <ytlib/cypress_client/public.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TGetRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath Path;
    std::vector<Stroka> Attributes;

    TGetRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("attributes", Attributes)
            .Default();
    }
};

typedef TIntrusivePtr<TGetRequest> TGetRequestPtr;

class TGetCommand
    : public TTransactedCommandBase<TGetRequest>
{
public:
    explicit TGetCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
   { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TSetRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath Path;

    TSetRequest()
    {
        RegisterParameter("path", Path);
    }
};

typedef TIntrusivePtr<TSetRequest> TSetRequestPtr;

class TSetCommand
    : public TTransactedCommandBase<TSetRequest>
{
public:
    explicit TSetCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TRemoveRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath Path;
    bool Recursive;
    bool Force;

    TRemoveRequest()
    {
        RegisterParameter("path", Path);
        // TODO(ignat): fix all places that use true default value
        // and change default value to false
        RegisterParameter("recursive", Recursive)
            .Default(true);
        RegisterParameter("force", Force)
            .Default(false);
    }
};

typedef TIntrusivePtr<TRemoveRequest> TRemoveRequestPtr;

class TRemoveCommand
    : public TTransactedCommandBase<TRemoveRequest>
{
public:
    explicit TRemoveCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TListRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath Path;
    std::vector<Stroka> Attributes;

    TListRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("attributes", Attributes)
            .Default();
    }
};

typedef TIntrusivePtr<TListRequest> TListRequestPtr;

class TListCommand
    : public TTransactedCommandBase<TListRequest>
{
public:
    explicit TListCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TCreateRequest
    : public TTransactedRequest
{
    TNullable<NYPath::TRichYPath> Path;
    NObjectClient::EObjectType Type;
    NYTree::INodePtr Attributes;
    bool Recursive;
    bool IgnoreExisting;

    TCreateRequest()
    {
        RegisterParameter("path", Path)
            .Default(Null);
        RegisterParameter("type", Type);
        RegisterParameter("attributes", Attributes)
            .Default(nullptr);
        RegisterParameter("recursive", Recursive)
            .Default(false);
        RegisterParameter("ignore_existing", IgnoreExisting)
            .Default(false);
    }
};

typedef TIntrusivePtr<TCreateRequest> TCreateRequestPtr;

class TCreateCommand
    : public TTransactedCommandBase<TCreateRequest>
{
public:
    explicit TCreateCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
   { }

private:
    virtual void DoExecute() override;
};

////////////////////////////////////////////////////////////////////////////////

struct TLockRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath Path;
    NCypressClient::ELockMode Mode;

    TLockRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("mode", Mode)
            .Default(NCypressClient::ELockMode::Exclusive);
    }
};

typedef TIntrusivePtr<TLockRequest> TLockRequestPtr;

class TLockCommand
    : public TTransactedCommandBase<TLockRequest>
{
public:
    explicit TLockCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TCopyRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath SourcePath;
    NYPath::TRichYPath DestinationPath;

    TCopyRequest()
    {
        RegisterParameter("source_path", SourcePath);
        RegisterParameter("destination_path", DestinationPath);
    }
};

typedef TIntrusivePtr<TCopyRequest> TCopyRequestPtr;

class TCopyCommand
    : public TTransactedCommandBase<TCopyRequest>
{
public:
    explicit TCopyCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TMoveRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath SourcePath;
    NYPath::TRichYPath DestinationPath;

    TMoveRequest()
    {
        RegisterParameter("source_path", SourcePath);
        RegisterParameter("destination_path", DestinationPath);
    }
};

typedef TIntrusivePtr<TMoveRequest> TMoveRequestPtr;

class TMoveCommand
    : public TTransactedCommandBase<TMoveRequest>
{
public:
    explicit TMoveCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TExistsRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath Path;

    TExistsRequest()
    {
        RegisterParameter("path", Path);
    }
};

typedef TIntrusivePtr<TExistsRequest> TExistsRequestPtr;

class TExistsCommand
    : public TTransactedCommandBase<TExistsRequest>
{
public:
    explicit TExistsCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
   { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TLinkRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath LinkPath;
    NYPath::TRichYPath TargetPath;
    NYTree::INodePtr Attributes;
    bool Recursive;
    bool IgnoreExisting;

    TLinkRequest()
    {
        RegisterParameter("link_path", LinkPath);
        RegisterParameter("target_path", TargetPath);
        RegisterParameter("attributes", Attributes)
            .Default(nullptr);
        RegisterParameter("recursive", Recursive)
            .Default(false);
        RegisterParameter("ignore_existing", IgnoreExisting)
            .Default(false);
    }
};

typedef TIntrusivePtr<TLinkRequest> TLinkRequestPtr;

class TLinkCommand
    : public TTransactedCommandBase<TLinkRequest>
{
public:
    explicit TLinkCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////
} // namespace NDriver
} // namespace NYT

