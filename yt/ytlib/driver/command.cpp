#include "stdafx.h"
#include "command.h"

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NObjectClient;
using namespace NScheduler;
using namespace NTransactionClient;
using namespace NMetaState;

////////////////////////////////////////////////////////////////////////////////

TCommandBase::TCommandBase()
    : Context(nullptr)
    , Replied(false)
{ }

void TCommandBase::Prepare()
{
    ObjectProxy.reset(new TObjectServiceProxy(Context->GetMasterChannel()));
    SchedulerProxy.reset(new TSchedulerServiceProxy(Context->GetSchedulerChannel()));
}

void TCommandBase::ReplyError(const TError& error)
{
    YCHECK(!Replied);
    YCHECK(!error.IsOK());

    Context->GetResponse()->Error = error;
    Replied = true;
}

void TCommandBase::ReplySuccess(const TYsonString& yson)
{
    YCHECK(!Replied);

    Consume(yson, ~Context->CreateOutputConsumer());

    Context->GetResponse()->Error = TError();
    Replied = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
