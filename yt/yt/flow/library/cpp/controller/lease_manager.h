#pragma once

#include "public.h"

namespace NYT::NFlow::NController {

struct ILeaseManager
    : public NYT::TRefCounted
{
    //! Opens the iteration: whatever keeps the existing leases alive, plus whatever the two calls
    //! below need to know. The transaction backend attaches to (and expires) the lease
    //! transactions; the dyntable one rewrites the pipeline-wide deadline row and reads the lease
    //! table. Must run before the layout of the iteration is mutated.
    virtual void CheckLeases(const TFlowViewPtr& flowView) = 0;

    //! Grants a lease to every job of the layout that does not already hold one. Throws if any
    //! job stays unleased: the layout must not be persisted with a job nobody can fence.
    virtual void PrepareLeases(const TFlowViewPtr& flowView) = 0;

    //! Revokes every lease that no job of the layout holds. Must run in the same iteration that
    //! dropped those jobs and before the layout is persisted: a partition whose job is gone from
    //! the layout while its lease is still live can end up running two jobs at once.
    virtual void TerminateStrayLeases(const TFlowViewPtr& flowView) = 0;
};

DEFINE_REFCOUNTED_TYPE(ILeaseManager);

ILeaseManagerPtr CreateLeaseManager(
    IYTConnectorPtr connector,
    TLeaseManagerConfigPtr config,
    bool dyntableLeases,
    i64 maxWritesPerTransaction);

} // namespace NYT::NFlow::NController
