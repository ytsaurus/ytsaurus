#include "stdafx.h"
#include "command.h"

#include <ytlib/security_client/rpc_helpers.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

TUntypedCommandBase::TUntypedCommandBase(ICommandContext* context)
    : Context(context)
    , Replied(false)
{ }

void TUntypedCommandBase::Prepare()
{
    ObjectProxy.Reset(new TObjectServiceProxy(Context->GetMasterChannel()));
    SchedulerProxy.Reset(new TSchedulerServiceProxy(Context->GetSchedulerChannel()));
}

void TUntypedCommandBase::ReplyError(const TError& error)
{
    YCHECK(!Replied);
    YCHECK(!error.IsOK());

    Context->GetResponse()->Error = error;
    Replied = true;
}

void TUntypedCommandBase::ReplySuccess(const TYsonString& yson)
{
    YCHECK(!Replied);

    auto consumer = Context->CreateOutputConsumer();
    Consume(yson, ~consumer);

    Context->GetResponse()->Error = TError();
    Replied = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
