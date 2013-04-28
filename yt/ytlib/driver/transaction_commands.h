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
    , public TTransactionalCommandMixin
    , public TMutatingCommandMixin
{
public:
    explicit TStartTransactionCommand(ICommandContext* context)
        : TTypedCommandBase(context)
        , TTransactionalCommandMixin(context, Request)
        , TMutatingCommandMixin(context, Request)
    { }

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
    , public TTransactionalCommandMixin
{
public:
    explicit TPingTransactionCommand(ICommandContext* context)
        : TTypedCommandBase(context)
        , TTransactionalCommandMixin(context, Request)
    { }

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
    , public TTransactionalCommandMixin
    , public TMutatingCommandMixin
{
public:
    explicit TCommitTransactionCommand(ICommandContext* context)
        : TTypedCommandBase(context)
        , TTransactionalCommandMixin(context, Request)
        , TMutatingCommandMixin(context, Request)
    { }

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
    , public TTransactionalCommandMixin
    , public TMutatingCommandMixin
{
public:
    explicit TAbortTransactionCommand(ICommandContext* context)
        : TTypedCommandBase(context)
        , TTransactionalCommandMixin(context, Request)
        , TMutatingCommandMixin(context, Request)
    { }

private:
    virtual void DoExecute();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

