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

    virtual TCommandDescriptor GetDescriptor();

private:
    virtual void DoExecute(TStartRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

struct TRenewTransactionRequest
    : public TTransactedRequest
{ };

typedef TIntrusivePtr<TRenewTransactionRequest> TRenewRequestPtr;

class TRenewTransactionCommand
    : public TTypedCommandBase<TRenewTransactionRequest>
{
public:
    explicit TRenewTransactionCommand(ICommandHost* host)
        : TTypedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

    virtual TCommandDescriptor GetDescriptor();

private:
    virtual void DoExecute(TRenewRequestPtr request);
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

    virtual TCommandDescriptor GetDescriptor();

private:
    virtual void DoExecute(TCommitRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

struct TAbortTransactionRequest
    : public TTransactedRequest
{ };

typedef TIntrusivePtr<TAbortTransactionRequest> TAbortTransactionRequestPtr;

class TAbortTransactionCommand
    : public TTypedCommandBase<TAbortTransactionRequest>
{
public:
    explicit TAbortTransactionCommand(ICommandHost* host)
        : TTypedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

    virtual TCommandDescriptor GetDescriptor();

private:
    virtual void DoExecute(TAbortTransactionRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

