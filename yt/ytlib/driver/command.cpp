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

    Context->SetResponse(TDriverResponse(error));
    Replied = true;
}

void TCommandBase::ReplySuccess()
{
    YCHECK(!Replied);

    Context->SetResponse(TDriverResponse(TError()));
    Replied = true;
}

void TCommandBase::ReplySuccess(const TYsonString& yson)
{
    YCHECK(!Replied);

    auto consumer = Context->CreateOutputConsumer();
    Consume(yson, ~consumer);

    Context->SetResponse(TDriverResponse(TError()));
    Replied = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
