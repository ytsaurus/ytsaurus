#include "stdafx.h"
#include "command.h"

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NObjectClient;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

void TCommandBase::Prepare()
{
    //ObjectProxy.reset(new TObjectServiceProxy(Context->GetClient()->GetMasterChannel()));
    SchedulerProxy.reset(new TSchedulerServiceProxy(Context_->GetClient()->GetSchedulerChannel()));
}

void TCommandBase::Reply(const TError& error)
{
    YCHECK(!Replied_);
    YCHECK(!error.IsOK());

    Context_->Reply(error);
    Replied_ = true;
}

void TCommandBase::Reply()
{
    YCHECK(!Replied_);

    Context_->Reply(TError());
    Replied_ = true;
}

void TCommandBase::Reply(const TYsonString& yson)
{
    YCHECK(!Replied_);

    auto consumer = Context_->CreateOutputConsumer();
    Consume(yson, consumer.get());

    Context_->Reply(TError());
    Replied_ = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
