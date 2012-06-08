#include "transaction_executors.h"

#include <ytlib/job_proxy/config.h>
#include <ytlib/driver/driver.h>

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TStartTxExecutor::BuildArgs(IYsonConsumer* consumer)
{
    TTransactedExecutor::BuildArgs(consumer);
    BuildOptions(consumer);
}

Stroka TStartTxExecutor::GetDriverCommandName() const
{
    return "start_tx";
}

//////////////////////////////////////////////////////////////////////////////////

TRenewTxExecutor::TRenewTxExecutor()
    : TTransactedExecutor(true)
{ }

Stroka TRenewTxExecutor::GetDriverCommandName() const
{
    return "renew_tx";
}

//////////////////////////////////////////////////////////////////////////////////

TCommitTxExecutor::TCommitTxExecutor()
    : TTransactedExecutor(true)
{ }

Stroka TCommitTxExecutor::GetDriverCommandName() const
{
    return "commit_tx";
}

//////////////////////////////////////////////////////////////////////////////////

TAbortTxExecutor::TAbortTxExecutor()
    : TTransactedExecutor(true)
{ }

Stroka TAbortTxExecutor::GetDriverCommandName() const
{
    return "abort_tx";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
