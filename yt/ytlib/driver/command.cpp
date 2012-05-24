#include "stdafx.h"
#include "command.h"

namespace NYT {
namespace NDriver {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TUntypedCommandBase::TUntypedCommandBase(ICommandContext* context)
    : Context(context)
    , Replied(false)
{ }

void TUntypedCommandBase::ReplyError(const TError& error)
{
    YASSERT(!Replied);
    YASSERT(!error.IsOK());

    Context->GetResponse()->Error = error;
    Replied = true;
}

void TUntypedCommandBase::ReplySuccess(const TYson& yson)
{
    YASSERT(!Replied);

    auto consumer = Context->CreateOutputConsumer();
    ParseYson(yson, ~consumer);

    Context->GetResponse()->Error = TError();
    Replied = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
