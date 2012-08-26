#include "transaction_executors.h"

#include <server/job_proxy/config.h>
#include <ytlib/driver/driver.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

Stroka TStartTxExecutor::GetCommandName() const
{
    return "start_tx";
}

//////////////////////////////////////////////////////////////////////////////////

TRenewTxExecutor::TRenewTxExecutor()
    : TTransactedExecutor(true)
{ }

Stroka TRenewTxExecutor::GetCommandName() const
{
    return "renew_tx";
}

//////////////////////////////////////////////////////////////////////////////////

TCommitTxExecutor::TCommitTxExecutor()
    : TTransactedExecutor(true)
{ }

Stroka TCommitTxExecutor::GetCommandName() const
{
    return "commit_tx";
}

//////////////////////////////////////////////////////////////////////////////////

TAbortTxExecutor::TAbortTxExecutor()
    : TTransactedExecutor(true)
{ }

Stroka TAbortTxExecutor::GetCommandName() const
{
    return "abort_tx";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
