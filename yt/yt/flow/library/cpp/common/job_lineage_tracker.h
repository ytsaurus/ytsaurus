#pragma once

#include "public.h"
#include "traverse.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Records completed processing observations in the worker lineage statistics.
struct IJobLineageTracker
    : public TRefCounted
{
    virtual void Add(TLineageDelta delta) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobLineageTracker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
