#pragma once

#include "command.h"

#include <ytlib/ytree/public.h>
#include <ytlib/object_server/id.h>
#include <ytlib/cypress/id.h>

namespace NYT {
namespace NDriver {
    
////////////////////////////////////////////////////////////////////////////////

struct TGetRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;

    TGetRequest()
    {
        Register("path", Path);
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
    virtual void DoExecute();

};

////////////////////////////////////////////////////////////////////////////////

struct TSetRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;
    // Note: Value is passed via StdIn

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
    virtual void DoExecute();
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
    virtual void DoExecute();
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
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

struct TCreateRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;
    NObjectServer::EObjectType Type;

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
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

struct TLockRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;
    NCypress::ELockMode Mode;

    TLockRequest()
    {
        Register("path", Path);
        Register("mode", Mode)
            .Default(NCypress::ELockMode::Exclusive);
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
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////


} // namespace NDriver
} // namespace NYT

