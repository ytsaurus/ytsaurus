#include "stdafx.h"
#include "command.h"

#include <ytlib/api/client.h>

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
    Client = NApi::CreateClient(Context);
    ObjectProxy.reset(new TObjectServiceProxy(Context->GetMasterChannel()));
    SchedulerProxy.reset(new TSchedulerServiceProxy(Context->GetSchedulerChannel()));
}

void TCommandBase::Reply(const TError& error)
{
    YCHECK(!Replied);
    YCHECK(!error.IsOK());

    Context->Response() = TDriverResponse(error);
    Replied = true;
}

void TCommandBase::Reply()
{
    YCHECK(!Replied);

    Context->Response() = TDriverResponse(TError());
    Replied = true;
}

void TCommandBase::Reply(const TYsonString& yson)
{
    YCHECK(!Replied);

    auto consumer = Context->CreateOutputConsumer();
    Consume(yson, consumer.get());

    Context->Response() = TDriverResponse(TError());
    Replied = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
