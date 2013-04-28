#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TStartTransactionRequest
    : public TTransactionalRequest
    , public TMutatingRequest
{
    TNullable<TDuration> Timeout;
    NYTree::INodePtr Attributes;

    TStartTransactionRequest()
    {
        RegisterParameter("timeout", Timeout)
            .Default(Null);
        RegisterParameter("attributes", Attributes)
            .Default(nullptr);
    }
};

typedef TIntrusivePtr<TStartTransactionRequest> TStartRequestPtr;

class TStartTransactionCommand
    : public TTypedCommandBase<TStartTransactionRequest>
    , public TTransactionalCommand
    , public TMutatingCommand
{
private:
    virtual void DoExecute();

};

////////////////////////////////////////////////////////////////////////////////

struct TPingTransactionRequest
    : public TTransactionalRequest
{ };

typedef TIntrusivePtr<TPingTransactionRequest> TRenewRequestPtr;

class TPingTransactionCommand
    : public TTypedCommandBase<TPingTransactionRequest>
    , public TTransactionalCommand
{
private:
    virtual void DoExecute();

};

////////////////////////////////////////////////////////////////////////////////

struct TCommitTransactionRequest
    : public TTransactionalRequest
    , public TMutatingRequest
{ };

typedef TIntrusivePtr<TCommitTransactionRequest> TCommitRequestPtr;

class TCommitTransactionCommand
    : public TTypedCommandBase<TCommitTransactionRequest>
    , public TTransactionalCommand
    , public TMutatingCommand
{
private:
    virtual void DoExecute();

};

////////////////////////////////////////////////////////////////////////////////

struct TAbortTransactionRequest
    : public TTransactionalRequest
    , public TMutatingRequest
{ };

typedef TIntrusivePtr<TAbortTransactionRequest> TAbortTransactionRequestPtr;

class TAbortTransactionCommand
    : public TTypedCommandBase<TAbortTransactionRequest>
    , public TTransactionalCommand
    , public TMutatingCommand
{
private:
    virtual void DoExecute();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

