#include "runtime_init_context.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

IExternalStateJoinerPtr IRuntimeInitContext::ForwardExternalStateJoiner(
    const IRuntimeInitContextPtr& context,
    const std::string& name)
{
    return context->GetExternalStateJoinerOrThrow(name);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
