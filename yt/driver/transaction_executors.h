#pragma once

#include "executor.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TStartTxExecutor
    : public TTransactedExecutor
{
private:
    virtual Stroka GetCommandName() const;
};

//////////////////////////////////////////////////////////////////////////////////

class TRenewTxExecutor
    : public TTransactedExecutor
{
public:
    TRenewTxExecutor();

private:
    virtual Stroka GetCommandName() const;
};

//////////////////////////////////////////////////////////////////////////////////

class TCommitTxExecutor
    : public TTransactedExecutor
{
public:
    TCommitTxExecutor();

private:
    virtual Stroka GetCommandName() const;
};

//////////////////////////////////////////////////////////////////////////////////

class TAbortTxExecutor
    : public TTransactedExecutor
{
public:
    TAbortTxExecutor();

private:
    virtual Stroka GetCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
