#include "transaction_executors.h"

#include <core/ytree/fluent.h>

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
    , ForceArg("", "force", "force abort in any state", false)
{
    CmdLine.add(ForceArg);
}

Stroka TAbortTransactionExecutor::GetCommandName() const
{
    return "abort_tx";
}

void TAbortTransactionExecutor::BuildParameters(NYson::IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("force").Value(ForceArg.getValue());

    TTransactedExecutor::BuildParameters(consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
