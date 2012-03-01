#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TStartTransactionRequest
    : public TRequestBase
{
    NYTree::INodePtr Manifest;

    TStartTransactionRequest()
    {
        Register("manifest", Manifest)
            .Default();
    }
};

class TStartTransactionCommand
    : public TCommandBase<TStartTransactionRequest>
{
public:
    TStartTransactionCommand(IDriverImpl* driverImpl)
        : TCommandBase(driverImpl)
    { }

private:
    virtual void DoExecute(TStartTransactionRequest* request);
};

////////////////////////////////////////////////////////////////////////////////

struct TCommitTransactionRequest
    : public TTransactedRequest
{ };

class TCommitTransactionCommand
    : public TCommandBase<TCommitTransactionRequest>
{
public:
    TCommitTransactionCommand(IDriverImpl* driverImpl)
        : TCommandBase(driverImpl)
    { }

private:
    virtual void DoExecute(TCommitTransactionRequest* request);
};

////////////////////////////////////////////////////////////////////////////////

struct TAbortTransactionRequest
    : public TTransactedRequest
{ };

class TAbortTransactionCommand
    : public TCommandBase<TAbortTransactionRequest>
{
public:
    TAbortTransactionCommand(IDriverImpl* driverImpl)
        : TCommandBase(driverImpl)
    { }

private:
    virtual void DoExecute(TAbortTransactionRequest* request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

