#pragma once

#include "public.h"

#include "message.h"
#include "timer.h"

#include <library/cpp/yt/memory/ref_counted.h>

#include <optional>
#include <vector>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Sink for messages and timers produced by a computation's (or process function's)
//! processing logic. The worker backs it with TRootOutputCollector / TOutputCollector
//! (see library/cpp/computation); tests back it with a recording implementation.
struct IOutputCollector
    : public TRefCounted
{
    // TODO(YTFLOW-500): drop the default and migrate downstream 2-arg callers.
    [[nodiscard]] virtual IOutputCollectorPtr SetParents(
        const std::vector<TInputMessageConstPtr>& messages,
        const std::vector<TInputTimerConstPtr>& timers,
        const std::vector<TInputVisitConstPtr>& visits = {}) = 0;
    //! Adds an output message. For a source computation, |distribute| = false keeps the
    //! message out of the downstream output while still letting it advance the watermark; for
    //! other computations, |distribute| = false simply drops the message (it is not emitted).
    virtual void AddMessage(TMessage&& message, bool distribute = true) = 0;

    virtual void AddTimer(TSystemTimestamp triggerTimestamp, std::optional<TSystemTimestamp> eventTimestamp = {}) = 0;
    virtual void AddTimer(const TStreamId& streamId, TSystemTimestamp triggerTimestamp, std::optional<TSystemTimestamp> eventTimestamp = {}) = 0;
    virtual void AddTimer(TTimer&& timer) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOutputCollector);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
