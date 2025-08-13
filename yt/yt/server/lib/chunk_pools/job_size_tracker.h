#pragma once

#include "private.h"
#include "resource.h"

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TJobSizeTrackerOptions
{
    //! The options below control a special size overflow mode in which limits increase in a geometric progression.
    //! This mode is used in CHYT for early exit-like optimizations.
    //! If ratio is set, resources specified in the geometric resource vector are multiplied by the ratio during
    //! each flush at most `LimitProgressionLength` times, skipping the first `LimitProgressionOffset` flushes.
    std::optional<double> LimitProgressionRatio;
    std::vector<EResourceKind> GeometricResources;
    int LimitProgressionLength = 1;
    int LimitProgressionOffset = 0;
};

struct IJobSizeTracker
{
    //! Account slice resource vector.
    virtual void AccountSlice(TResourceVector vector) = 0;

    //! Given a row sliceable data slice resource vector, return its fraction which is ok to
    //! be included in current job without overflow. This method is intended for using before
    //! calling #AccountSlice.
    //! After doing so, you must still call #AccountSlice for newly formed data slices.
    virtual double SuggestRowSplitFraction(TResourceVector vector) const = 0;

    //! If current job plus possible extraStatistics is large enough to be flushed, returns true; false otherwise.
    virtual std::optional<std::any> CheckOverflow(TResourceVector extraVector = TResourceVector()) const = 0;

    //! Called to indicate the fact currently building job was flushed.
    virtual void Flush(const std::optional<std::any>& overflowToken) = 0;

    virtual ~IJobSizeTracker() = default;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IJobSizeTracker> CreateJobSizeTracker(
    TResourceVector limitVector,
    TJobSizeTrackerOptions options,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
