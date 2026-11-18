#include "runtime_init_context.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void IRuntimeInitContext::RegisterAsyncProcessor(TAsyncProcessorDescriptorPtr descriptor) const
{
    THROW_ERROR_EXCEPTION("Async processors are not supported by this host of the process function")
        .With("async_processor", descriptor->Name);
}

IRuntimeContextPtr IRuntimeInitContext::GetResultHandlerContext() const
{
    THROW_ERROR_EXCEPTION("Async processors are not supported by this host of the process function");
}

IExternalStateJoinerPtr IRuntimeInitContext::ForwardExternalStateJoiner(
    const IRuntimeInitContextPtr& context,
    const std::string& name)
{
    return context->GetExternalStateJoinerOrThrow(name);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
