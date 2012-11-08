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
    explicit TStartTransactionCommand(const ICommandContextPtr& context)
        : TTransactedCommandBase(context)
        , TUntypedCommandBase(context)
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
    explicit TRenewTransactionCommand(const ICommandContextPtr& context)
        : TTransactedCommandBase(context)
        , TUntypedCommandBase(context)
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
    explicit TCommitTransactionCommand(const ICommandContextPtr& context)
        : TTransactedCommandBase(context)
        , TUntypedCommandBase(context)
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
    explicit TAbortTransactionCommand(const ICommandContextPtr& context)
        : TTransactedCommandBase(context)
        , TUntypedCommandBase(context)
    { }

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

