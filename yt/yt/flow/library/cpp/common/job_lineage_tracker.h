#pragma once

#include "public.h"
#include "traverse.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Holds lineage produced by the current job epoch until that epoch is committed.
struct IJobLineageTracker
    : public TRefCounted
{
    virtual void Add(TLineageDelta delta) = 0;
    virtual void Commit() = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobLineageTracker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
