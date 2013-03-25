#include "transaction_executors.h"

#include <server/job_proxy/config.h>
#include <ytlib/driver/driver.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TStartTxExecutor::TStartTxExecutor()
    : TTransactedExecutor(false, true)
{ }

Stroka TStartTxExecutor::GetCommandName() const
{
    return "start_tx";
}

//////////////////////////////////////////////////////////////////////////////////

TPingTxExecutor::TPingTxExecutor()
    : TTransactedExecutor(true, false)
{ }

Stroka TPingTxExecutor::GetCommandName() const
{
    return "ping_tx";
}

//////////////////////////////////////////////////////////////////////////////////

TCommitTxExecutor::TCommitTxExecutor()
    : TTransactedExecutor(true, false)
{ }

Stroka TCommitTxExecutor::GetCommandName() const
{
    return "commit_tx";
}

//////////////////////////////////////////////////////////////////////////////////

TAbortTxExecutor::TAbortTxExecutor()
    : TTransactedExecutor(true, false)
{ }

Stroka TAbortTxExecutor::GetCommandName() const
{
    return "abort_tx";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
