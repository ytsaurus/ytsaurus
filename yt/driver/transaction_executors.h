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
private:
    virtual Stroka GetDriverCommandName() const;
};

//////////////////////////////////////////////////////////////////////////////////

class TCommitTxExecutor
    : public TTransactedExecutor
{
private:
    virtual Stroka GetDriverCommandName() const;
};

//////////////////////////////////////////////////////////////////////////////////

class TAbortTxExecutor
    : public TTransactedExecutor
{
private:
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
