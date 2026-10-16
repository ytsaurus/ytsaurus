#pragma once

#include "public.h"

#include "source_base.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct IOrderedSource
    : public virtual ISource
{
    virtual TMessageId GetMaxPersistedMessageIdExclusive() = 0;

    // Returns alignment timestamp delta between first not persisted message and last read message.
    virtual TDuration GetAlignmentTimestampWindow() = 0;

    //! Returns the next to read alignment timestamp.
    virtual TSystemTimestamp GetReadAlignmentTimestamp() = 0;
};

DEFINE_REFCOUNTED_TYPE(IOrderedSource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
