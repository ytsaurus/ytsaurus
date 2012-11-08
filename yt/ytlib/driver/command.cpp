#include "stdafx.h"
#include "command.h"

namespace NYT {
namespace NDriver {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TUntypedCommandBase::TUntypedCommandBase(const ICommandContextPtr& context)
    : Context(context)
{ }

void TUntypedCommandBase::ReplyError(const TError& error)
{
    auto promise = Context->GetResponsePromise();
    YASSERT(!promise.IsSet());
    YASSERT(!error.IsOK());

    promise.Set(TDriverResponse({ error }));
}

void TUntypedCommandBase::ReplySuccess(const TYsonString& yson)
{
    auto promise = Context->GetResponsePromise();
    YASSERT(!promise.IsSet());

    auto consumer = Context->CreateOutputConsumer();
    Consume(yson, ~consumer);

    promise.Set(TDriverResponse());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
