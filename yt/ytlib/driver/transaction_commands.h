#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TStartTransactionRequest
    : public TTransactionalRequest
    , public TMutatingRequest
    , public TPrerequisiteRequest
{
    TNullable<TDuration> Timeout;
    NYTree::INodePtr Attributes;

    TStartTransactionRequest()
    {
        RegisterParameter("timeout", Timeout)
            .Default();
        RegisterParameter("attributes", Attributes)
            .Default(nullptr);
    }
};

class TStartTransactionCommand
    : public TTypedCommand<TStartTransactionRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TPingTransactionRequest
    : public TTransactionalRequest
{ };

class TPingTransactionCommand
    : public TTypedCommand<TPingTransactionRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TCommitTransactionRequest
    : public TTransactionalRequest
    , public TMutatingRequest
{ };

class TCommitTransactionCommand
    : public TTypedCommand<TCommitTransactionRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TAbortTransactionRequest
    : public TTransactionalRequest
    , public TMutatingRequest
{
    bool Force;

    TAbortTransactionRequest()
    {
        RegisterParameter("force", Force)
            .Default(false);
    }
};

class TAbortTransactionCommand
    : public TTypedCommand<TAbortTransactionRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

