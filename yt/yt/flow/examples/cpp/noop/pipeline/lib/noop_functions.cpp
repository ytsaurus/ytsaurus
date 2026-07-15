#include "noop_functions.h"

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

void TNoopFunction::ProcessMessage(
    const TInputMessageConstPtr& /*message*/,
    const IOutputCollectorPtr& /*output*/,
    const IRuntimeContextPtr& /*context*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
