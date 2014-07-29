#include "transaction_executors.h"

#include <server/job_proxy/config.h>
#include <ytlib/driver/driver.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TStartTransactionExecutor::TStartTransactionExecutor()
    : TTransactedExecutor(false, true)
{ }

Stroka TStartTransactionExecutor::GetCommandName() const
{
    return "start_tx";
}

//////////////////////////////////////////////////////////////////////////////////

TPingTransactionExecutor::TPingTransactionExecutor()
    : TTransactedExecutor(true, false)
{ }

Stroka TPingTransactionExecutor::GetCommandName() const
{
    return "ping_tx";
}

//////////////////////////////////////////////////////////////////////////////////

TCommitTransactionExecutor::TCommitTransactionExecutor()
    : TTransactedExecutor(true, false)
{ }

Stroka TCommitTransactionExecutor::GetCommandName() const
{
    return "commit_tx";
}

//////////////////////////////////////////////////////////////////////////////////

TAbortTransactionExecutor::TAbortTransactionExecutor()
    : TTransactedExecutor(true, false)
{ }

Stroka TAbortTransactionExecutor::GetCommandName() const
{
    return "abort_tx";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
