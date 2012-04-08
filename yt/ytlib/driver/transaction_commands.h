#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TStartTransactionRequest
    : public TTransactedRequest
{ };

typedef TIntrusivePtr<TStartTransactionRequest> TStartRequestPtr;

class TStartTransactionCommand
    : public TTypedCommandBase<TStartTransactionRequest>
{
public:
    explicit TStartTransactionCommand(ICommandHost* host)
        : TTypedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

private:
    virtual void DoExecute(TStartRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

struct TCommitTransactionRequest
    : public TTransactedRequest
{ };

typedef TIntrusivePtr<TCommitTransactionRequest> TCommitRequestPtr;

class TCommitTransactionCommand
    : public TTypedCommandBase<TCommitTransactionRequest>
{
public:
    explicit TCommitTransactionCommand(ICommandHost* host)
        : TTypedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

private:
    virtual void DoExecute(TCommitRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

struct TAbortRequest
    : public TTransactedRequest
{ };

typedef TIntrusivePtr<TAbortRequest> TAbortRequestPtr;

class TAbortTransactionCommand
    : public TTypedCommandBase<TAbortRequest>
{
public:
    explicit TAbortTransactionCommand(ICommandHost* host)
        : TTypedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

private:
    virtual void DoExecute(TAbortRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

