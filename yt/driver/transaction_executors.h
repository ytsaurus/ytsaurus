#pragma once

#include "executor.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TStartTransactionExecutor
    : public TTransactedExecutor
{
public:
    TStartTransactionExecutor();

private:
    virtual Stroka GetCommandName() const override;
};

//////////////////////////////////////////////////////////////////////////////////

class TPingTransactionExecutor
    : public TTransactedExecutor
{
public:
    TPingTransactionExecutor();

private:
    virtual Stroka GetCommandName() const override;
};

//////////////////////////////////////////////////////////////////////////////////

class TCommitTransactionExecutor
    : public TTransactedExecutor
{
public:
    TCommitTransactionExecutor();

private:
    virtual Stroka GetCommandName() const override;

};

//////////////////////////////////////////////////////////////////////////////////

class TAbortTransactionExecutor
    : public TTransactedExecutor
{
public:
    TAbortTransactionExecutor();

private:
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
