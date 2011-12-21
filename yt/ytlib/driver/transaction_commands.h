#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TStartTransactionRequest
    : TRequestBase
{ };

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
    : TRequestBase
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
    : TRequestBase
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

