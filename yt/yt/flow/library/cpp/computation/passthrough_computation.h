#pragma once

#include "public.h"

#include "computation_base.h"
#include "swift_map_computation.h"
#include "swift_ordered_source_computation.h"
#include "transform_computation.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TPassthroughComputation
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) final;
    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) final;
};

DEFINE_REFCOUNTED_TYPE(TPassthroughComputation);

////////////////////////////////////////////////////////////////////////////////

class TSwiftPassthroughOrderedSourceComputation
    : public TSwiftOrderedSourceComputation
{
public:
    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    void DoInit(IJobInitContextPtr initContext) final;
    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) final;
};

DEFINE_REFCOUNTED_TYPE(TSwiftPassthroughOrderedSourceComputation);

////////////////////////////////////////////////////////////////////////////////

class TSwiftPassthroughComputation
    : public TSwiftMapComputation
{
public:
    using TSwiftMapComputation::TSwiftMapComputation;

    void DoInit(IJobInitContextPtr initContext) final;
    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) final;
};

DEFINE_REFCOUNTED_TYPE(TSwiftPassthroughComputation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
