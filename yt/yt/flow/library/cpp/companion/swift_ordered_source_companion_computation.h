#pragma once

#include "companion_computation_base.h"
#include "public.h"

#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

// Create custom TStreamSpecs enriched with source streams schemas.
TStreamSpecsPtr CreateLocalStreamSpecs(
    const THashMap<TStreamId, NTableClient::TTableSchemaPtr>& sourceStreamsSchemas,
    const THashSet<TStreamId>& outputStreamIds,
    const TStreamSpecsPtr& streamSpecs);

////////////////////////////////////////////////////////////////////////////////

class TSwiftOrderedSourceCompanionComputation
    : public TCompanionComputationBaseAdapter<TSwiftOrderedSourceComputation>
{
public:
    TSwiftOrderedSourceCompanionComputation(
        TComputationContextPtr context,
        TDynamicComputationContextPtr dynamicContext);

    void DoInit(IJobInitContextPtr initContext) override;

    void DoProcess(IInputContextPtr input, IOutputCollectorPtr output) override;
};

DEFINE_REFCOUNTED_TYPE(TSwiftOrderedSourceCompanionComputation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
