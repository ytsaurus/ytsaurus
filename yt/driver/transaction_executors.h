#pragma once

#include "executor.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TStartTxExecutor
    : public TTransactedExecutor
{
public:
    TStartTxExecutor();

private:
    virtual Stroka GetCommandName() const override;
};

//////////////////////////////////////////////////////////////////////////////////

class TPingTxExecutor
    : public TTransactedExecutor
{
public:
    TPingTxExecutor();

private:
    virtual Stroka GetCommandName() const override;
};

//////////////////////////////////////////////////////////////////////////////////

class TCommitTxExecutor
    : public TTransactedExecutor
{
public:
    TCommitTxExecutor();

private:
    virtual Stroka GetCommandName() const override;

};

//////////////////////////////////////////////////////////////////////////////////

class TAbortTxExecutor
    : public TTransactedExecutor
{
public:
    TAbortTxExecutor();

private:
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
