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
    : public TTransactedCommandBase<TStartTransactionRequest>
{
public:
    explicit TStartTransactionCommand(ICommandContext* host)
        : TTransactedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

struct TRenewTransactionRequest
    : public TTransactedRequest
{ };

typedef TIntrusivePtr<TRenewTransactionRequest> TRenewRequestPtr;

class TRenewTransactionCommand
    : public TTransactedCommandBase<TRenewTransactionRequest>
{
public:
    explicit TRenewTransactionCommand(ICommandContext* host)
        : TTransactedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

struct TCommitTransactionRequest
    : public TTransactedRequest
{ };

typedef TIntrusivePtr<TCommitTransactionRequest> TCommitRequestPtr;

class TCommitTransactionCommand
    : public TTransactedCommandBase<TCommitTransactionRequest>
{
public:
    explicit TCommitTransactionCommand(ICommandContext* host)
        : TTransactedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

struct TAbortTransactionRequest
    : public TTransactedRequest
{ };

typedef TIntrusivePtr<TAbortTransactionRequest> TAbortTransactionRequestPtr;

class TAbortTransactionCommand
    : public TTransactedCommandBase<TAbortTransactionRequest>
{
public:
    explicit TAbortTransactionCommand(ICommandContext* host)
        : TTransactedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

