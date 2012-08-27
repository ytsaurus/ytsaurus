#pragma once

#include "command.h"

#include <ytlib/ytree/public.h>
#include <ytlib/cypress_client/public.h>

namespace NYT {
namespace NDriver {
    
////////////////////////////////////////////////////////////////////////////////

struct TGetRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;
    std::vector<Stroka> Attributes;

    TGetRequest()
    {
        Register("path", Path);
        Register("attributes", Attributes);
    }
};

typedef TIntrusivePtr<TGetRequest> TGetRequestPtr;

class TGetCommand
    : public TTransactedCommandBase<TGetRequest>
{
public:
    explicit TGetCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
        , TUntypedCommandBase(context)
   { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TSetRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;

    TSetRequest()
    {
        Register("path", Path);
    }
};

typedef TIntrusivePtr<TSetRequest> TSetRequestPtr;

class TSetCommand
    : public TTransactedCommandBase<TSetRequest>
{
public:
    explicit TSetCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
        , TUntypedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TRemoveRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;

    TRemoveRequest()
    {
        Register("path", Path);
    }
};

typedef TIntrusivePtr<TRemoveRequest> TRemoveRequestPtr;

class TRemoveCommand
    : public TTransactedCommandBase<TRemoveRequest>
{
public:
    explicit TRemoveCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
        , TUntypedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TListRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;

    TListRequest()
    {
        Register("path", Path);
    }
};

typedef TIntrusivePtr<TListRequest> TListRequestPtr;

class TListCommand
    : public TTransactedCommandBase<TListRequest>
{
public:
    explicit TListCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
        , TUntypedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TCreateRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;
    NObjectClient::EObjectType Type;

    TCreateRequest()
    {
        Register("path", Path);
        Register("type", Type);
    }
};

typedef TIntrusivePtr<TCreateRequest> TCreateRequestPtr;

class TCreateCommand
    : public TTransactedCommandBase<TCreateRequest>
{
public:
    explicit TCreateCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
        , TUntypedCommandBase(context)
   { }

private:
    virtual void DoExecute() override;
};

////////////////////////////////////////////////////////////////////////////////

struct TLockRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;
    NCypressClient::ELockMode Mode;

    TLockRequest()
    {
        Register("path", Path);
        Register("mode", Mode)
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
        , TUntypedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TCopyRequest
    : public TTransactedRequest
{
    NYTree::TYPath SourcePath;
    NYTree::TYPath DestinationPath;

    TCopyRequest()
    {
        Register("source_path", SourcePath);
        Register("destination_path", DestinationPath);
    }
};

typedef TIntrusivePtr<TCopyRequest> TCopyRequestPtr;

class TCopyCommand
    : public TTransactedCommandBase<TCopyRequest>
{
public:
    explicit TCopyCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
        , TUntypedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TMoveRequest
    : public TTransactedRequest
{
    NYTree::TYPath SourcePath;
    NYTree::TYPath DestinationPath;

    TMoveRequest()
    {
        Register("source_path", SourcePath);
        Register("destination_path", DestinationPath);
    }
};

typedef TIntrusivePtr<TMoveRequest> TMoveRequestPtr;

class TMoveCommand
    : public TTransactedCommandBase<TMoveRequest>
{
public:
    explicit TMoveCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
        , TUntypedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

