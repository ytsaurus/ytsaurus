#include "stdafx.h"
#include "command.h"
#include "connection.h"

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NObjectClient;
using namespace NScheduler;

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

    Context->Response() = TDriverResponse(error);
    Replied = true;
}

void TCommandBase::ReplySuccess()
{
    YCHECK(!Replied);

    Context->Response() = TDriverResponse(TError());
    Replied = true;
}

void TCommandBase::ReplySuccess(const TYsonString& yson)
{
    YCHECK(!Replied);

    auto consumer = Context->CreateOutputConsumer();
    Consume(yson, ~consumer);

    Context->Response() = TDriverResponse(TError());
    Replied = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
