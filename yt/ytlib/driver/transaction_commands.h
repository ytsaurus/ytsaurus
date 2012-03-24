#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TStartRequest
    : public TRequestBase
{
    NYTree::INodePtr Manifest;

    TStartRequest()
    {
        Register("manifest", Manifest)
            .Default();
    }
};

typedef TIntrusivePtr<TStartRequest> TStartRequestPtr;

class TStartCommand
    : public TCommandBase<TStartRequest>
{
public:
    TStartCommand(IDriverImpl* driverImpl)
        : TCommandBase(driverImpl)
    { }

private:
    virtual void DoExecute(TStartRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

struct TCommitRequest
    : public TTransactedRequest
{ };

typedef TIntrusivePtr<TCommitRequest> TCommitRequestPtr;

class TCommitCommand
    : public TCommandBase<TCommitRequest>
{
public:
    TCommitCommand(IDriverImpl* driverImpl)
        : TCommandBase(driverImpl)
    { }

private:
    virtual void DoExecute(TCommitRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

struct TAbortRequest
    : public TTransactedRequest
{ };

typedef TIntrusivePtr<TAbortRequest> TAbortRequestPtr;

class TAbortCommand
    : public TCommandBase<TAbortRequest>
{
public:
    TAbortCommand(IDriverImpl* driverImpl)
        : TCommandBase(driverImpl)
    { }

private:
    virtual void DoExecute(TAbortRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

