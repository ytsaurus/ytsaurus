#pragma once

#include "executor.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TStartTxExecutor
    : public TTransactedExecutor
{
private:
    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

//////////////////////////////////////////////////////////////////////////////////

class TRenewTxExecutor
    : public TTransactedExecutor
{
public:
    TRenewTxExecutor();

private:
    virtual Stroka GetDriverCommandName() const;
};

//////////////////////////////////////////////////////////////////////////////////

class TCommitTxExecutor
    : public TTransactedExecutor
{
public:
    TCommitTxExecutor();

private:
    virtual Stroka GetDriverCommandName() const;
};

//////////////////////////////////////////////////////////////////////////////////

class TAbortTxExecutor
    : public TTransactedExecutor
{
public:
    TAbortTxExecutor();

private:
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
