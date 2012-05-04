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
    : public TTypedCommandBase<TGetRequest>
{
public:
    explicit TGetCommand(ICommandHost* host)
        : TTypedCommandBase(host)
        , TUntypedCommandBase(host)
   { }

    virtual TCommandDescriptor GetDescriptor();

private:
    virtual void DoExecute(TGetRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

struct TSetRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;
    NYTree::INodePtr Value;

    TSetRequest()
    {
        Register("path", Path);
        Register("value", Value); //TODO(panin): Think about defaulting this value
    }
};

typedef TIntrusivePtr<TSetRequest> TSetRequestPtr;

class TSetCommand
    : public TTypedCommandBase<TSetRequest>
{
public:
    explicit TSetCommand(ICommandHost* host)
        : TTypedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

    virtual TCommandDescriptor GetDescriptor();

private:
    virtual void DoExecute(TSetRequestPtr request);
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
    : public TTypedCommandBase<TRemoveRequest>
{
public:
    explicit TRemoveCommand(ICommandHost* host)
        : TTypedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

    virtual TCommandDescriptor GetDescriptor();

private:
    virtual void DoExecute(TRemoveRequestPtr request);
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
    : public TTypedCommandBase<TListRequest>
{
public:
    explicit TListCommand(ICommandHost* host)
        : TTypedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

    virtual TCommandDescriptor GetDescriptor();

private:
    virtual void DoExecute(TListRequestPtr request);
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
    : public TTypedCommandBase<TCreateRequest>
{
public:
    explicit TCreateCommand(ICommandHost* host)
        : TTypedCommandBase(host)
        , TUntypedCommandBase(host)
   { }

    virtual TCommandDescriptor GetDescriptor();

private:
    virtual void DoExecute(TCreateRequestPtr request);
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
    : public TTypedCommandBase<TLockRequest>
{
public:
    explicit TLockCommand(ICommandHost* host)
        : TTypedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

    virtual TCommandDescriptor GetDescriptor();

private:
    virtual void DoExecute(TLockRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////


} // namespace NDriver
} // namespace NYT

