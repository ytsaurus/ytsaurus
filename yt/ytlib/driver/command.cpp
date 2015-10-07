#include "stdafx.h"
#include "command.h"

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NObjectClient;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

void TCommandBase::Prepare()
{ }

void TCommandBase::Reply(const TYsonString& yson)
{
    auto consumer = Context_->CreateOutputConsumer();
    Consume(yson, consumer.get());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
