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

Stroka TRenewTxExecutor::GetDriverCommandName() const
{
    return "renew_tx";
}

//////////////////////////////////////////////////////////////////////////////////

Stroka TCommitTxExecutor::GetDriverCommandName() const
{
    return "commit_tx";
}

//////////////////////////////////////////////////////////////////////////////////

Stroka TAbortTxExecutor::GetDriverCommandName() const
{
    return "abort_tx";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
