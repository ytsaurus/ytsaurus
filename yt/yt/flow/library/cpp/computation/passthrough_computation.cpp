#include "passthrough_computation.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TPassthroughComputation::DoInit(IJobInitContextPtr /*initContext*/)
{
    if (GetSpec()->OutputStreamIds.size() != 1) {
        THROW_ERROR_EXCEPTION("Expecting to have exactly one output stream, but found %v",
            GetSpec()->OutputStreamIds.size());
    }
    if (GetSpec()->TimerStreams.size() != 0) {
        THROW_ERROR_EXCEPTION("Expecting to have exactly zero timer streams, but found %v",
            GetSpec()->TimerStreams.size());
    }
    if (GetSpec()->KeyVisitorStreams.size() != 0) {
        THROW_ERROR_EXCEPTION("Expecting to have exactly zero key_visitor_streams, but found %v",
            GetSpec()->KeyVisitorStreams.size());
    }
}

void TPassthroughComputation::DoProcessMessage(const TMessage& message, IOutputCollectorPtr output)
{
    output->AddMessage(ConvertToOutputMessage(message));
}

////////////////////////////////////////////////////////////////////////////////

void TSwiftPassthroughOrderedSourceComputation::DoInit(IJobInitContextPtr /*initContext*/)
{
    if (GetSpec()->OutputStreamIds.size() != 1) {
        THROW_ERROR_EXCEPTION("Expecting to have exactly one output stream, but found %v",
            GetSpec()->OutputStreamIds.size());
    }
}

void TSwiftPassthroughOrderedSourceComputation::DoProcessMessage(const TMessage& message, IOutputCollectorPtr output)
{
    output->AddMessage(ConvertToOutputMessage(message));
}

////////////////////////////////////////////////////////////////////////////////

void TSwiftPassthroughComputation::DoInit(IJobInitContextPtr /*initContext*/)
{
    if (GetSpec()->OutputStreamIds.size() != 1) {
        THROW_ERROR_EXCEPTION("Expecting to have exactly one output stream, but found %v",
            GetSpec()->OutputStreamIds.size());
    }
}

void TSwiftPassthroughComputation::DoProcessMessage(const TMessage& message, IOutputCollectorPtr output)
{
    output->AddMessage(ConvertToOutputMessage(message));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
