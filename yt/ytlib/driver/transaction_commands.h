#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TStartTransactionCommand
    : public TTypedCommand<NApi::TTransactionStartOptions>
{
public:
    TStartTransactionCommand();

private:
    NTransactionClient::ETransactionType Type;
    NYTree::INodePtr Attributes;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TPingTransactionCommand
    : public TTypedCommand<NApi::TTransactionalOptions>
{
private:
    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCommitTransactionCommand
    : public TTypedCommand<NApi::TTransactionCommitOptions>
{
private:
    void DoExecute(ICommandContextPtr context);
};

////////////////////////////////////////////////////////////////////////////////

class TAbortTransactionCommand
    : public TTypedCommand<NApi::TTransactionAbortOptions>
{
public:
    TAbortTransactionCommand();

private:
    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

struct TGenerateTimestampOptions
{ };

class TGenerateTimestampCommand
    : public TTypedCommand<TGenerateTimestampOptions>
{
private:
    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

