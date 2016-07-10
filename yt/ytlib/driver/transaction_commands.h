#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TStartTransactionCommand
    : public TTypedCommand<NApi::TTransactionStartOptions>
{
private:
    NTransactionClient::ETransactionType Type;
    NYTree::INodePtr Attributes;

public:
    TStartTransactionCommand()
    {
        RegisterParameter("type", Type)
            .Default(NTransactionClient::ETransactionType::Master);
        RegisterParameter("attributes", Attributes)
            .Default(nullptr);
        RegisterParameter("sticky", Options.Sticky)
            .Default(false);
        RegisterParameter("timeout", Options.Timeout)
            .Optional();
        RegisterParameter("transaction_id", Options.ParentId)
            .Optional();
        RegisterParameter("ping_ancestor_transactions", Options.PingAncestors)
            .Default(false);
    }

    void Execute(ICommandContextPtr context);
};

class TPingTransactionCommand
    : public TTypedCommand<NApi::TTransactionalOptions>
{
public:
    void Execute(ICommandContextPtr context);
};

class TCommitTransactionCommand
    : public TTypedCommand<NApi::TTransactionCommitOptions>
{
public:
    void Execute(ICommandContextPtr context);
};

class TAbortTransactionCommand
    : public TTypedCommand<NApi::TTransactionAbortOptions>
{
public:
    TAbortTransactionCommand()
    {
        RegisterParameter("force", Options.Force)
            .Optional();
    }

    void Execute(ICommandContextPtr context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

